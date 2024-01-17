#!/bin/bash

sleep 120
ansible-playbook -i inventory.yaml reconf-01.yaml

sleep 120
ansible-playbook -i inventory.yaml reconf-02.yaml

sleep 120
ansible-playbook -i inventory.yaml reconf-00.yaml
