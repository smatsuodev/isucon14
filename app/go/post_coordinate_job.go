package main

import (
	"context"
	"database/sql"
	"errors"
	"github.com/oklog/ulid/v2"
	"log/slog"
	"time"
)

type PostCoordinateJobData struct {
	Chair                   *Chair
	ChairLocationCoordinate *Coordinate
	RecordedAt              time.Time
}

var postCoordinateJobChan = make(chan *PostCoordinateJobData, 1000)

func postCoordinateJobWorker() {
	for {
		select {
		case data := <-postCoordinateJobChan:
			go performPostCoordinate(data)
		}
	}
}

func performPostCoordinate(data *PostCoordinateJobData) {
	ctx := context.Background()

	chair := data.Chair
	latitude := data.ChairLocationCoordinate.Latitude
	longitude := data.ChairLocationCoordinate.Longitude

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
		`INSERT INTO chair_locations (id, chair_id, latitude, longitude, created_at) VALUES (?, ?, ?, ?, ?)`,
		chairLocationID, chair.ID, latitude, longitude, data.RecordedAt,
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
			if latitude == ride.PickupLatitude && longitude == ride.PickupLongitude && status == "ENROUTE" {
				if _, err := tx.ExecContext(ctx, "INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)", ulid.Make().String(), ride.ID, "PICKUP"); err != nil {
					slog.Error(err.Error())
					return
				}
			}

			if latitude == ride.DestinationLatitude && longitude == ride.DestinationLongitude && status == "CARRYING" {
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
