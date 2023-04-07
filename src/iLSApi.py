from sodapy import Socrata


class IowaLiquorSalesAPI:
    """
    A class that provides access to the Iowa Liquor Sales API.

    Attributes:
        client: A sodapy.Socrata object that is used to interact with the API.

    Methods:
        query_sales(params): Retrieves records from the Iowa Liquor Sales API based on the input parameters.
    """

    def __init__(self, client=None, api_key: str = None) -> None:
        """
        Initializes a new IowaLiquorSalesAPI instance.

        Parameters:
            client (optional): A sodapy.Socrata object that is used to interact with the API.
        """
        self.client = client or Socrata("data.iowa.gov", None)

    def query_sales(self, params):
        """
        Retrieves records from the Iowa Liquor Sales API based on the input parameters.

        Parameters:
            params: A dictionary containing query parameters for the API.
                Valid keys include:
                    - start_date (optional): A datetime.date object representing the start date of the date range.
                    - end_date (optional): A datetime.date object representing the end date of the date range.
                    - store_numbers (optional): A list of store numbers to filter by.
                    - invoice_numbers (optional): A list of invoice numbers to filter by.
                    - cities (optional): A list of cities to filter by.
                    - zip_codes (optional): A list of zip codes to filter by.
                    - product_ids (optional): A list of product IDs to filter by.
                    - vendor_ids (optional): A list of vendor IDs to filter by.

        Returns:
            A list of records retrieved from the Iowa Liquor Sales API based on the input parameters.
        """
        # Set parameters
        """start_date = params.get("start_date")
        end_date = params.get("end_date")
        store_numbers = params.get("store_numbers", [])
        invoice_numbers = params.get("invoice_numbers", [])
        cities = params.get("cities", [])
        zip_codes = params.get("zip_codes", [])
        product_ids = params.get("product_ids", [])
        vendor_ids = params.get("vendor_ids", [])
        """
        # Construct query
        query = ""
        for K in params.keys():
            query += "{} in ({}) and".format(
                K, ",".join([f"'{v}'" for v in params.get(K)])
            )
        """if start_date:
            query += "date >= '{}' and ".format(start_date)
        if end_date:
            query += "date <= '{}' and ".format(end_date)
        if store_numbers:
            query += "store_number in ({}) and ".format(
                )
            )
        if invoice_numbers:
            query += "invoice_number in ({}) and ".format(
                ",".join([f"'{v}'" for v in invoice_numbers])
            )
        if cities:
            query += "city in ({}) and ".format(",".join([f"'{v}'" for v in cities]))
        if zip_codes:
            query += "zip_code in ({}) and ".format(
                ",".join([f"'{v}'" for v in zip_codes])
            )
        if product_ids:
            query += "item_number in ({}) and ".format(
                ",".join([f"'{v}'" for v in product_ids])
            )
        if vendor_ids:
            query += "vendor_number in ({}) and ".format(
                ",".join([f"'{v}'" for v in vendor_ids])
            )"""

        # Remove trailing "and" from query
        query = query[:-5]

        # Retrieve records with query
        results = self.client.get("m3tr-qhgy", where=query)

        return results
