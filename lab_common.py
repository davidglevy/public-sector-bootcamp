class LabContext:
    """
        Helper class for the lab - you can create helpful classes inline or externally and import as a module.
    """

    def __init__(self, spark):
        """
            Initialize with our current_user and catalog for the lab work.
        """
        self.current_user = spark.sql("SELECT current_user() AS current_user").collect()[0]['current_user']
        print(f"Running as [{self.current_user}]")

        self.catalog = self.current_user.split("@")[0].replace(".", "_") + "_lab"
        print(f"Catalog will be [{self.catalog}]")

        self.spark = spark

    def runSql(self, task, sql):
        print(f"Performing task [{task}] with SQL: {sql}")
        self.spark.sql(sql)
        

    def setupLab(self, labName):
        """
           Deletes old schema and creates new one with lab name
        """
        self.runSql("Deleting old lab catalog/schema (if exists)", f"DROP CATALOG IF EXISTS {self.catalog} CASCADE")

        print("Creating schema for lab")
        self.runSql("Creating Catalog", f"CREATE CATALOG {self.catalog}")
        self.runSql("Creating Schema", f"CREATE SCHEMA {self.catalog}.{labName}")
        self.runSql("Setting default schema", f"USE {self.catalog}.{labName}")