#!/bin/bash

form=4

sudo nvme format /dev/nvme0n1 --lbaf=${form} -f #--ms=1 
sudo nvme format /dev/nvme1n1 --lbaf=${form} -f #--ms=1
sudo nvme format /dev/nvme2n1 --lbaf=${form} -f #--ms=1
sudo nvme format /dev/nvme3n1 --lbaf=${form} -f #--ms=1
