#!/bin/bash
#ansible-playbook -i inventory.yaml noler-00.yaml

sleep 120
ansible-playbook -i inventory.yaml noler-01.yaml

sleep 120
ansible-playbook -i inventory.yaml noler-02.yaml

sleep 120
ansible-playbook -i inventory.yaml noler-00.yaml
