# Dune API Python Client

THIS PROJECT IS DEPRECATED, PLEASE USE INSTEAD: [dune-client](https://github.com/cowprotocol/dune-client)
PyPI package: https://pypi.org/project/dune-client/

This is a python client to use the Dune API to execute queries and read the results. The Dune API is currently in Beta. And this client is also under active development.

## Setup


1. If you don’t have Python installed, [install it from here](https://www.python.org/downloads/)

2. Clone this repository

3. Navigate into the project directory

   ```bash
   $ cd duneapi-client-python
   ```

4. Create a new virtual environment

   ```bash
   $ python -m venv venv
   $ . venv/bin/activate
   ```

5. Install the requirements

   ```bash
   $ pip install -r requirements.txt
   ```

6. Add your Dune API key to your local environement.

   ```bash
   $ export DUNE_API_KEY="YOUR_API_KEY"
   ```

7. Run the `duneapi.py` script, pass the ID of the query you would like to execute and get results from as an command line argument. We have added an example query ID for reference.

   ```bash
   $ python duneapi.py 1258228
   ```

You should now be able to see the status of your query being executed on the command line.
