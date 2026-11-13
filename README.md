# Run the application
1. Go to `kotlin_kinesis_streams_flink_demo` and run `mvn clean install`
2. Then back on the root run `docker-compose up -d`
3. Visit http://localhost:8081 for the Flink dashboard
4. Send POST requests to http://localhost:8080/api/appuser with body `{ "username": "some-user", "email": "some-email" }` and then to http://localhost:8080/api/userrole with body `{ "username": "some-user", "role": "some-role" }`
5. Check the logs in the `kinesis-app` backend to see the messages