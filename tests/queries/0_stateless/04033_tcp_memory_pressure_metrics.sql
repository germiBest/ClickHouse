SELECT value > 0 FROM system.asynchronous_metrics WHERE name = 'TCPSocketsInUse';
SELECT value > 0 FROM system.asynchronous_metrics WHERE name = 'TCPSocketsAllocated';
SELECT value >= 0 FROM system.asynchronous_metrics WHERE name = 'TCPSocketsMemoryPages';
SELECT value > 0 FROM system.asynchronous_metrics WHERE name = 'TCPMemoryPressureThreshold';
SELECT value > 0 FROM system.asynchronous_metrics WHERE name = 'TCPMemoryHighThreshold';
