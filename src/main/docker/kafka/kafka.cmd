kafka-topics --bootstrap-server localhost:9092 --topic orders-by-user --create
kafka-topics --bootstrap-server localhost:9092 --topic discount-profiles-by-user --create --config "cleanup.policy=compact"
kafka-topics --bootstrap-server localhost:9092 --topic discounts --create --config "cleanup.policy=compact"
kafka-topics --bootstrap-server localhost:9092 --topic orders --create
kafka-topics --bootstrap-server localhost:9092 --topic payments --create
kafka-topics --bootstrap-server localhost:9092 --topic paid-orders --create

kafka-console-producer --topic discounts --broker-list localhost:9092 --property parse.key=true --property key.separator=,
kafka-console-producer --topic discount-profiles-by-user --broker-list localhost:9092 --property parse.key=true --property key.separator=,
kafka-console-producer --topic orders-by-user --broker-list localhost:9092 --property parse.key=true --property key.separator=,
kafka-console-producer --topic payments --broker-list localhost:9092 --property parse.key=true --property key.separator=,