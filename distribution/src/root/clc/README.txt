# Hazelcast CLC Installer

The `install.sh` script in this directory downloads and installs the latest stable release of Hazelcast CLC.

## What is Hazelcast CLC?

Hazelcast CLC is a command-line tool for connecting to and interact with clusters on Hazelcast Viridian Cloud and Hazelcast Platform directly from the command line or through scripts.

More information about Hazelcast CLC is at:
https://docs.hazelcast.com/clc/latest/overview

## Usage

The install script requires one of the following platforms:
* Linux AMD64
* Linux ARM
* Linux ARM64
* macOS AMD64 (Intel)
* macOS ARM64 (M1/M2)

Run the `install.sh` script using Bash:

    $ bash ./install.sh

## Install Location

Hazelcast CLC is a single binary named `clc`, installed in `$HOME/.hazelcast/bin`.
It can be moved to another directory.

