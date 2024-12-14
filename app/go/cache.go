package main

import (
	"context"
	"database/sql"
	"github.com/samber/lo"
)

var cache *AppCache = nil

type ChairTotalDistance struct {
	ChairID                string       `db:"chair_id"`
	TotalDistance          int          `db:"total_distance"`
	TotalDistanceUpdatedAt sql.NullTime `db:"total_distance_updated_at"`
}

type AppCache struct {
	chairTotalDistances Cache[string, ChairTotalDistance]
	latestChairLocation Cache[string, ChairLocation]
	activeRides         Cache[string, int]
}

func NewAppCache(ctx context.Context) *AppCache {
	c := &AppCache{
		// chair が 530 くらい
		chairTotalDistances: lo.Must1(NewInMemoryLRUCache[string, ChairTotalDistance](1000)),
		latestChairLocation: lo.Must1(NewInMemoryLRUCache[string, ChairLocation](1000)),
		activeRides:         lo.Must1(NewInMemoryLRUCache[string, int](1000)),
	}

	// chairTotalDistances の初期化
	var totalDistances []ChairTotalDistance
	if err := db.Select(&totalDistances, `
		WITH tmp AS (
			SELECT chair_id,
				   created_at,
				   ABS(latitude - LAG(latitude) OVER (PARTITION BY chair_id ORDER BY created_at)) +
				   ABS(longitude - LAG(longitude) OVER (PARTITION BY chair_id ORDER BY created_at)) AS distance
			FROM chair_locations
		)
		SELECT chair_id,
			   SUM(IFNULL(distance, 0)) AS total_distance,
			   MAX(created_at)          AS total_distance_updated_at
		FROM tmp
		GROUP BY chair_id;
	`); err != nil {
		panic(err)
	}
	for _, totalDistance := range totalDistances {
		_ = c.chairTotalDistances.Set(context.Background(), totalDistance.ChairID, totalDistance)
	}

	var chairLocations []ChairLocation
	if err := db.Select(&chairLocations, `
		WITH tmp AS (
		    SELECT id, MAX(created_at) FROM chair_locations GROUP BY chair_id, id
		)
		SELECT * FROM chair_locations WHERE id IN (SELECT id FROM tmp)
	`); err != nil {
		panic(err)
	}
	for _, chairLocation := range chairLocations {
		_ = c.latestChairLocation.Set(context.Background(), chairLocation.ChairID, chairLocation)
	}

	var chairs []Chair
	db.Select(&chairs, `SELECT * FROM chairs`)

	for _, chair := range chairs {
		var rides []*Ride
		count := 0

		db.Select(&rides, `SELECT * FROM rides WHERE chair_id = ? ORDER BY created_at DESC`, chair.ID)

		for _, ride := range rides {
			// 過去にライドが存在し、かつ、それが完了していない場合はスキップ
			status := lo.Must1(getLatestRideStatus(ctx, db, ride.ID))

			if status != "COMPLETED" {
				count++
			}
		}

		c.activeRides.Set(ctx, chair.ID, count)
	}

	return c
}
