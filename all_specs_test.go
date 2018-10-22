package workers

func setupTestConfig() {
	Configure(Options{
		ServerAddr: "localhost:6379",
		ProcessID:  "1",
		Database:   15,
		PoolSize:   1,
	})

	rc := Config.Client
	rc.FlushDB().Result()
}

func setupTestConfigWithNamespace(namespace string) {
	Configure(Options{
		ServerAddr: "localhost:6379",
		ProcessID:  "1",
		Database:   15,
		PoolSize:   1,
		Namespace:  namespace,
	})

	rc := Config.Client
	rc.FlushDB().Result()
}
