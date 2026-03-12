package config

import (
	"os"
	"strconv"
	"sync"
)

// FeatureFlags holds all feature flags for the application
type FeatureFlags struct {
	useSTInterpolatePoint bool
	once                  sync.Once
}

// Global instance of feature flags
var flags = &FeatureFlags{}

// UseSTInterpolatePoint returns whether to use ST_InterpolatePoint for M-value calculation.
// Defaults to true for new installations.
// Can be overridden via environment variable ST_INTERPOLATE_POINT_ENABLED.
// When disabled, falls back to the original CTE-based implementation.
func UseSTInterpolatePoint() bool {
	flags.once.Do(func() {
		// Check environment variable
		if envVal := os.Getenv("ST_INTERPOLATE_POINT_ENABLED"); envVal != "" {
			if parsed, err := strconv.ParseBool(envVal); err == nil {
				flags.useSTInterpolatePoint = parsed
				return
			}
		}
		// Default to true for new installations
		flags.useSTInterpolatePoint = true
	})
	return flags.useSTInterpolatePoint
}

// SetUseSTInterpolatePoint sets the feature flag (useful for testing)
func SetUseSTInterpolatePoint(enabled bool) {
	flags.useSTInterpolatePoint = enabled
}
