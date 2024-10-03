# py-mapreduce

## Protocol

I designed this simple communication protocol between the Driver and the Workers which includes HTTP Post requests made by the server in which **we only include metadata**. In the case of the figure, suppose we have 1 Driver with N=4 Map tasks and M=3 Reuce tasks.

<img src="images/protocol.jpg" width="638" />