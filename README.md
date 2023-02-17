# Gossip Heartbeat
Goal of this project is to have scalable heartbeat mechanism.
By scalable, we mean smaller number of heartbeat messages to exchange between nodes.
Currently there are n*n heartbeat messages to spread live-ness info.
With this Gossip based approach, total message count to reach consensus 
will be n*fan-out*log(n)(fan-out base).

Number of message comparison:
Exponential                                      Gossip(fan-out is 5) 
100 nodes cluster: 100 * (100 -1) ≈ 10_000 msg   (5 cycle, 100 * 5 * 5 ≈ 2_500)
200 nodes cluster: 200 * (200 -1) ≈ 40_000 msg   (5 cycle, 200 * 5 * 5 ≈ 5_000)
300 nodes cluster: 300 * (300 -1) ≈ 90_000 msg  (6 cycle, 300 * 5 * 6 ≈ 9_000)
1000 nodes cluster: 1000 * (1000 -1) ≈ 1_000_000 msg (6 cycle, 1000 * 5 * 6 ≈ 30_000)


Problem is mostly visible in large clusters and in
there, message noise prevents spreading live-ness info. 

