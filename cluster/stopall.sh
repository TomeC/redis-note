#!/bin/bash
find . -name *.pid | xargs cat | xargs kill -9
