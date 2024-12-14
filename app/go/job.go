package main

import (
	"context"
	"database/sql"
	"errors"
	"github.com/oklog/ulid/v2"
)

type PostCoordinateRequest struct {
	ctx   context.Context
	chair *Chair
	req   *Coordinate
}

var postCoordinateCh = make(chan *PostCoordinateRequest, 1000)

func listenJobChannels(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-postCoordinateCh:
			chairPostCoordinateJob(req.ctx, req.chair, req.req)
		}
	}
}

func chairPostCoordinateJob(ctx context.Context, chair *Chair, req *Coordinate) {
	tx, err := db.Beginx()
	if err != nil {
		return
	}
	defer tx.Rollback()

	// キャッシュの更新のために取得
	lastLocation, _ := cache.latestChairLocation.Get(ctx, chair.ID)

	chairLocationID := ulid.Make().String()
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO chair_locations (id, chair_id, latitude, longitude) VALUES (?, ?, ?, ?)`,
		chairLocationID, chair.ID, req.Latitude, req.Longitude,
	); err != nil {
		return
	}

	location := &ChairLocation{}
	if err := tx.GetContext(ctx, location, `SELECT * FROM chair_locations WHERE id = ?`, chairLocationID); err != nil {
		return
	}

	// tx の失敗は考えない
	updateLatestLocationCache(ctx, location)
	updateTotalDistanceCache(ctx, lastLocation, location)

	ride := &Ride{}
	if err := tx.GetContext(ctx, ride, `SELECT * FROM rides WHERE chair_id = ? ORDER BY updated_at DESC LIMIT 1`, chair.ID); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return
		}
	} else {
		status, err := getLatestRideStatus(ctx, tx, ride.ID)
		if err != nil {
			return
		}
		if status != "COMPLETED" && status != "CANCELED" {
			if req.Latitude == ride.PickupLatitude && req.Longitude == ride.PickupLongitude && status == "ENROUTE" {
				if _, err := tx.ExecContext(ctx, "INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)", ulid.Make().String(), ride.ID, "PICKUP"); err != nil {
					return
				}
			}

			if req.Latitude == ride.DestinationLatitude && req.Longitude == ride.DestinationLongitude && status == "CARRYING" {
				if _, err := tx.ExecContext(ctx, "INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)", ulid.Make().String(), ride.ID, "ARRIVED"); err != nil {
					return
				}
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return
	}
}
