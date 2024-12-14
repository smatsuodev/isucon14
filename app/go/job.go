package main

import (
	"context"
	"database/sql"
	"errors"
	"github.com/oklog/ulid/v2"
)

type PostCoordinateRequest struct {
	Ctx      context.Context
	Location *ChairLocation
}

var postCoordinateCh = make(chan *PostCoordinateRequest, 1000)

func listenJobChannels(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-postCoordinateCh:
			chairPostCoordinateJob(req.Ctx, req.Location)
		}
	}
}

func chairPostCoordinateJob(ctx context.Context, location *ChairLocation) {
	tx, err := db.Beginx()
	if err != nil {
		return
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO chair_locations (id, chair_id, latitude, longitude, created_at) VALUES (?, ?, ?, ?, ?)`,
		location.ID, location.ChairID, location.Latitude, location.Longitude, location.CreatedAt,
	); err != nil {
		return
	}

	ride := &Ride{}
	if err := tx.GetContext(ctx, ride, `SELECT * FROM rides WHERE chair_id = ? ORDER BY updated_at DESC LIMIT 1`, location.ChairID); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return
		}
	} else {
		status, err := getLatestRideStatus(ctx, tx, ride.ID)
		if err != nil {
			return
		}
		if status != "COMPLETED" && status != "CANCELED" {
			if location.Latitude == ride.PickupLatitude && location.Longitude == ride.PickupLongitude && status == "ENROUTE" {
				if _, err := tx.ExecContext(ctx, "INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)", ulid.Make().String(), ride.ID, "PICKUP"); err != nil {
					return
				}
			}

			if location.Latitude == ride.DestinationLatitude && location.Longitude == ride.DestinationLongitude && status == "CARRYING" {
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
