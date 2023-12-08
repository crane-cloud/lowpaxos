#!/bin/bash
tc qdisc add dev eno1d1 root handle 1: tbf rate 1gbit buffer 1600 limit 3000
tc qdisc add dev eno1d1 parent 1:1 handle 10: netem delay 20ms loss 0.0000001%