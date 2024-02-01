#!/bin/bash
tc qdisc add dev enp65s0f0 root handle 1: tbf rate 1gbit buffer 1600 limit 3000
tc qdisc add dev enp65s0f0 parent 1:1 handle 10: netem delay 81ms loss 0.1%