package main

import (
	"context"
	"github.com/samber/lo"
	"time"
)

type TotalDistanceCache struct {
	Distance  int
	UpdatedAt time.Time
}

type AppCache struct {
	latestChairLocationCache Cache[string, ChairLocation]
	totalDistanceCache       Cache[string, TotalDistanceCache]
}

func NewCache() *AppCache {
	return &AppCache{
		latestChairLocationCache: lo.Must(NewInMemoryLRUCache[string, ChairLocation](1000)),
		totalDistanceCache:       lo.Must(NewInMemoryLRUCache[string, TotalDistanceCache](1000)),
	}
}

func initChairLocationCache(ctx context.Context, cache *AppCache) error {
	locations := []ChairLocation{}
	if err := db.SelectContext(ctx, &locations, `SELECT id,
	   chair_id,
	   latitude,
	   longitude,
	   created_at
FROM chair_locations
ORDER BY created_at ASC
`); err != nil {
		return err
	}

	for _, loc := range locations {
		updateChairLocation(ctx, cache, loc)
	}
	return nil
}

func updateChairLocation(ctx context.Context, cache *AppCache, location ChairLocation) error {
	prevLocationCache, _ := cache.latestChairLocationCache.Get(ctx, location.ChairID)
	cache.latestChairLocationCache.Set(ctx, location.ChairID, location)
	if prevLocationCache.Found {
		prevLocation := prevLocationCache.Value
		distance := abs(prevLocation.Latitude-location.Latitude) + abs(prevLocation.Longitude-location.Longitude)

		if distanceCache, _ := cache.totalDistanceCache.Get(ctx, location.ChairID); distanceCache.Found {
			d := TotalDistanceCache{
				Distance:  distanceCache.Value.Distance + distance,
				UpdatedAt: location.CreatedAt,
			}
			cache.totalDistanceCache.Set(ctx, location.ChairID, d)
		}
	} else {
		cache.totalDistanceCache.Set(ctx, location.ChairID, TotalDistanceCache{
			Distance:  0,
			UpdatedAt: location.CreatedAt,
		})
	}
	return nil
}
