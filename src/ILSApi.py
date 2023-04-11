from sodapy import Socrata
import logging


class IowaLiquorSalesAPI:
    """
    A class that provides access to the Iowa Liquor Sales API.

    Attributes:
        client: A sodapy.Socrata object that is used to interact with the API.

    Methods:
        query_sales(params): Retrieves records from the Iowa Liquor Sales API based on the input parameters.
    """

    def __init__(self, client=None, params: dict = None) -> None:
        """
        Initializes a new IowaLiquorSalesAPI instance.

        Parameters:logging.basicConfig(format='%(levelname)s:%(message)s'
            client (optional): A sodapy.Socrata object that is used to interact with the API.
            params (dict, optional): Additional paramets to pass to Socrata (if client is None). Defaults to None.
        """
        logging.info(
            f"ILSPAI called - Lclient passed {client}: params passed: {params}"
        )

        if params is None:
            params = {"app_token": None}

        self.client = client or Socrata("data.iowa.gov", **params)
        self.results = []

    def get_saved_results(self) -> list:
        """
        Returns any results from query_sales that have not been returned

        Returns:
            list of json
        """
        _results = self.results
        logging.info(f"how many records are to be returned: {len(_results)}")

        self.results = []

        return _results

    def query_sales(self, params: dict = {}, return_results: bool = True) -> list:
        """
        Retrieves records from the Iowa Liquor Sales API based on the input parameters.

        Parameters:
            return_reults (bool, optional): have query_sales return the result set instead of saving them. Defaults to Treu
            params: A dictionary containing query parameters for the API.
                Valid keys include:
                    - date (optional): A list of dates to filter by.
                    - invoice_line_no (optional): A list of invoice_line_no to filter by
                    - store (optional): A list of store numbers to filter by.
                    - name (option): A list of store names to filter by.
                    - address (option): a list of street address for stores to filter by.
                    - city (option): A list of cities to fitler by.
                    - zipecode (option): A list of zipcode to be filter by.
                    - county_number (option): A list of county numbers to filter by.
                    - vendor_no (option): A list of vendor number to filter by.
                    - vendor_name (option): A list of vendor names to filter by.
                    - itemno (option): A list of item number to filter by.
                    - im_desc (option): A list of item description to filter by.
                    - query (option): The complete where statement to apply. Will ignore all other keys if present.
        Returns:
            A list of json (records) retrieved from the Iowa Liquor Sales API based on the input parameters.
        """
        # Set parameters

        # Construct query
        logging.info(f"query_sales: params passed {params}")
        query = ""
        for K in params.keys():
            if K == "query":
                query = params.get(K)
                break
            query += "{} in ({}) and ".format(
                K, ",".join([f"'{v}'" for v in params.get(K)])
            )

        # Remove trailing "and" from query
        query = query[:-5]
        logging.info(f"query_sales: query string: {query}")

        # Retrieve records with query
        results = self.client.get("m3tr-qhgy", where=query)

        if not results:
            logging.info(f"size of return result set: {len(results)}")

        if return_results:
            return results
        else:
            self.client = self.client + results
