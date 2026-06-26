package config

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
)

func InitConfig() {
	viper.SetDefault("server", "api.killallchickens.org")
	viper.SetDefault("stun", "stun:stun.l.google.com:19302")
	viper.SetDefault("chunkSize", 64*1024) // 64KB


	var appConfigDir string
	configDir, err := os.UserConfigDir()
	if err == nil {
		appConfigDir = filepath.Join(configDir, "burrow")
		viper.AddConfigPath(appConfigDir)
	}

	configFilePath := filepath.Join(appConfigDir, "config.yaml")

	viper.SetConfigName("config")
	viper.SetConfigType("yaml")

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// fmt.Println("[Config] No config found. Generating default config file...")

			if err := os.MkdirAll(appConfigDir, os.ModePerm); err != nil {
				// fmt.Println("Failed to create config directory:", err)
				return
			}

			if err := viper.WriteConfigAs(configFilePath); err != nil {
				// fmt.Println("Failed to write default config file:", err)
			} else {
				// fmt.Println("[Config] Created default config file at:", configFilePath)
			}
		} else {
			// fmt.Println("[Config] Error reading existing config file:", err)
		}
	} else {
		// fmt.Println("[Config] Loaded file:", viper.ConfigFileUsed())
	}

	viper.SetEnvPrefix("BURR")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
}
