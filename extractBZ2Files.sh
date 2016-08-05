#!/bin/sh
for i in /home/bilickiv/raw_dataset/new_data/*.tar.bz2; do tar jxf $i -C /home/bilickiv/unzipped_dataset/; done