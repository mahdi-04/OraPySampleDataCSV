# Python Oracle Fetch Sample Data

Fetch Sample Data From Oracle Database To CSV

## CSV

In computing, a comma-separated values file is a delimited text file that uses a comma to separate values. 
A CSV file stores tabular data in plain text. 
Each line of the file is a data record. 
Each record consists of one or more fields, separated by commas

## Usage with Binary Install
```sh
/opt/OraPySampleDataCSV/main { schema-Name }.{ table-Name | view-Name } ~/configFile.ini ~/log/
```

## Usage with python Source

```sh
python3 main.py { schema-Name }.{ table-Name | view-Name } ~/configFile.ini ~/log/
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.


## License
[GPL Version 3](https://www.gnu.org/licenses/gpl-3.0.en.html/)