package main

import (
	"context"
	"database/sql"
	"errors"
	"github.com/oklog/ulid/v2"
	"log/slog"
)

type PostCoordinateJobData struct {
	chair                   *Chair
	chairLocationCoordinate *Coordinate
}

var postCoordinateJobChan = make(chan *PostCoordinateJobData, 5000)

func postCoordinateJobWorker() {
	for {
		select {
		case data := <-postCoordinateJobChan:
			performPostCoordinate(data.chair, data.chairLocationCoordinate)
		}
	}
}

func performPostCoordinate(chair *Chair, chairLocationCoordinate *Coordinate) {
	ctx := context.Background()

	tx, err := db.Beginx()
	if err != nil {
		slog.Error(err.Error())
		return
	}
	defer tx.Rollback()

	// キャッシュの更新のために取得
	lastLocation, _ := cache.latestChairLocation.Get(ctx, chair.ID)

	chairLocationID := ulid.Make().String()
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO chair_locations (id, chair_id, latitude, longitude) VALUES (?, ?, ?, ?)`,
		chairLocationID, chair.ID, chairLocationCoordinate.Latitude, chairLocationCoordinate.Longitude,
	); err != nil {
		slog.Error(err.Error())
		return
	}

	location := &ChairLocation{}
	if err := tx.GetContext(ctx, location, `SELECT * FROM chair_locations WHERE id = ?`, chairLocationID); err != nil {
		slog.Error(err.Error())
		return
	}

	// tx の失敗は考えない
	updateLatestLocationCache(ctx, location)
	updateTotalDistanceCache(ctx, lastLocation, location)

	ride := &Ride{}
	if err := tx.GetContext(ctx, ride, `SELECT * FROM rides WHERE chair_id = ? ORDER BY updated_at DESC LIMIT 1`, chair.ID); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			slog.Error(err.Error())
			return
		}
	} else {
		status, err := getLatestRideStatus(ctx, tx, ride.ID)
		if err != nil {
			slog.Error(err.Error())
			return
		}
		if status != "COMPLETED" && status != "CANCELED" {
			if chairLocationCoordinate.Latitude == ride.PickupLatitude && chairLocationCoordinate.Longitude == ride.PickupLongitude && status == "ENROUTE" {
				if _, err := tx.ExecContext(ctx, "INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)", ulid.Make().String(), ride.ID, "PICKUP"); err != nil {
					slog.Error(err.Error())
					return
				}
			}

			if chairLocationCoordinate.Latitude == ride.DestinationLatitude && chairLocationCoordinate.Longitude == ride.DestinationLongitude && status == "CARRYING" {
				if _, err := tx.ExecContext(ctx, "INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)", ulid.Make().String(), ride.ID, "ARRIVED"); err != nil {
					slog.Error(err.Error())
					return
				}
			}
		}
	}

	if err := tx.Commit(); err != nil {
		slog.Error(err.Error())
		return
	}
}
