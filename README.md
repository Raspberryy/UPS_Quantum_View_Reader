# UPS_Quantum_View_Reader
With these python scripts we show one possible implementation to query the UPS Quantum View APIⓇ, parse the results and safe data to a SQL server. Because this implementation was designed specifically for our needs, we might be missing some columns you have. Also, your API results might have a different structure, which currently will not be parsed by our scripts. The UPS Documentation from June 23 2021 was used for this implementation. 

## Design
This implementation uses SQLAlchemy as a OR Mapper which allows the python script to create and manage the database model as well as the data in it. Running the createDBModel() in qv_objects.py will create tables and columns for you. Once set up, running the qv_main.py will start querying the UPS Quantum View APIⓇ. When data is received and if needed stored to .json files, the results are merged to a single json object which is then parsed. Parsing is implemented in the qv_objects.py. Parsing the JSON results will create the objects which will later be inserted in the database. 

## Known Issues
To speed up parsing and minimize database size, the script caches Shipper and ShipTo data. Before parsing, all Shipper and ShipTo data is loaded to a cache. This not only takes time, but also consumes some memory. Depending on the amount of data this MIGHT lead to issues with a growing data. 

Once parsed, data is committed to the database server using the session.add_all() function. Usually this is where your script will spend most of its time. We are sure this can be handled in a more efficient way.
