package main

import (
	"context"
)

type AppCache struct {
	activeRides Cache[string, int]
}

var appCache *AppCache

func initCache(ctx context.Context) error {
	var err error
	activeRides, err := NewInMemoryLRUCache[string, int](10000)
	if err != nil {
		return err
	}

	appCache = &AppCache{
		activeRides: activeRides,
	}

	tx, err := db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var chairs []Chair
	err = tx.SelectContext(
		ctx,
		&chairs,
		`SELECT * FROM chairs`,
	)
	if err != nil {
		return err
	}

	for _, chair := range chairs {
		var rides []*Ride
		count := 0

		if err := tx.SelectContext(ctx, &rides, `SELECT * FROM rides WHERE chair_id = ? ORDER BY created_at DESC`, chair.ID); err != nil {
			return err
		}

		for _, ride := range rides {
			// 過去にライドが存在し、かつ、それが完了していない場合はスキップ
			status, err := getLatestRideStatus(ctx, tx, ride.ID)
			if err != nil {
				return err
			}

			if status != "COMPLETED" {
				count++
			}
		}

		activeRides.Set(ctx, chair.ID, count)
	}

	return tx.Commit()
}
