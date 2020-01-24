#!/bin/bash
yes | rm db_internal_janitor_clusters.zip 
7z a db_internal_janitor_clusters.zip *.py ./dep/* dbclient
