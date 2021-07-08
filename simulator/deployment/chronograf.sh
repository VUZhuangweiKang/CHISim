#!/bin/sh

wget https://dl.influxdata.com/chronograf/releases/chronograf_1.7.11_amd64.deb
sudo dpkg -i chronograf_1.7.11_amd64.deb
sudo rm -f hronograf_1.7.11_amd64.deb
sudo systemctl enable chronograf.service
sudo systemctl --system daemon-reload
sudo systemctl start chronograf.service