import boto3
import configparser
import time


##### HELPER FUNCTIONS #####

class RedshiftCluster:
    """
    Redshift keys
    """

    def __init__(self):
        """
        Initialize Redshift keys
        """
        config = configparser.ConfigParser()
        config.read('dwh.cfg')

        self.KEY = config.get('AWS','KEY')
        self.SECRET = config.get('AWS','SECRET')

        self.CLUSTER_TYPE = config.get("CLUSTER", "CLUSTER_TYPE")
        self.NUM_NODES = int(config.get("CLUSTER", "NUM_NODES"))
        self.NODE_TYPE = config.get("CLUSTER", "NODE_TYPE")

        self.HOST = config.get("CLUSTER", "HOST")
        self.DB_IDENTIFIER = config.get("CLUSTER", "DB_IDENTIFIER")
        self.DB_NAME = config.get("CLUSTER", "DB_NAME")
        self.DB_USER = config.get("CLUSTER", "DB_USER")
        self.DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
        self.DB_PORT = config.get("CLUSTER", "DB_PORT")

        self.ARN = config.get("IAM_ROLE", "ARN")

    def create_redshift_cluster(self):
        """
        Create a Redshift cluster
        """
        redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=self.KEY,
                            aws_secret_access_key=self.SECRET)

        iam = boto3.client('iam', region_name="us-west-2",
                           aws_access_key_id=self.KEY,
                           aws_secret_access_key=self.SECRET)



        try:
            response = redshift.create_cluster(
                #HW
                ClusterType=self.CLUSTER_TYPE,
                NodeType=self.NODE_TYPE,
                NumberOfNodes=self.NUM_NODES,

                #Identifiers & Credentials
                DBName=self.DB_NAME,
                ClusterIdentifier=self.DB_IDENTIFIER,
                MasterUsername=self.DB_USER,
                MasterUserPassword=self.DB_PASSWORD,

                #Roles (for s3 access)

                IamRoles=[self.ARN]
            )
        except Exception as e:
            print(e)

        return response

    def delete_redshift_cluster(self):
        """
        Delete a Redshift cluster
        """
        redshift = boto3.client('redshift')
        try:
            response = redshift.delete_cluster(
                ClusterIdentifier=self.DB_IDENTIFIER,
                SkipFinalClusterSnapshot=True
            )
        except Exception as e:
            print(e)

        return response

    def get_redshift_cluster_props(self):
        """
        Get Redshift cluster properties
        """
        redshift = boto3.client('redshift')
        try:
            response = redshift.describe_clusters(ClusterIdentifier=self.DB_IDENTIFIER)
        except Exception as e:
            print(e)
            return None

        return response

    def get_redshift_cluster_endpoint(self):
        """
        Get Redshift cluster endpoint
        """
        redshift = boto3.client('redshift')
        try:
            response = redshift.describe_clusters(ClusterIdentifier=self.DB_IDENTIFIER)
        except Exception as e:
            print(e)

        return response['Clusters'][0]['Endpoint']['Address']

    def get_redshift_cluster_role_arn(self):
        """
        Get Redshift cluster role arn
        """
        redshift = boto3.client('redshift')
        try:
            response = redshift.describe_clusters(ClusterIdentifier=self.DB_IDENTIFIER)
        except Exception as e:
            print(e)


        return response['Clusters'][0]['IamRoles'][0]['IamRoleArn']

    # print the redshift cluster endpoint and role arn
    def __repr__(self):
        return f"\n============ Redshift Cluster ============\n" \
               f"RedshiftClusterEndPoint={self.HOST}\n" \
               f"RedshiftIdentifier={self.DB_IDENTIFIER}\n" \
               f"RedshiftUser={self.DB_USER}\n" \
               f"RedshiftIAMRole={self.ARN}" \



if __name__ == "__main__":
    redshift_cluster = RedshiftCluster()
    # create a redshift cluster
    print(redshift_cluster)
    # new_cluster = redshift_cluster.create_redshift_cluster()
    # print(f"\n New redshift cluster created?\n {new_cluster}")

    # wait for the redshift cluster to be available
    while True:
        redshift_cluster_props = redshift_cluster.get_redshift_cluster_props()
        if redshift_cluster_props['Clusters'][0]['ClusterStatus'] == 'available':
            print(f"\n Redshift cluster is available!\n {redshift_cluster_props}")
            break
        time.sleep(60)

    # delete the redshift cluster
    redshift_cluster.delete_redshift_cluster()
    print(f"\n Redshift cluster deleted?\n {redshift_cluster.delete_redshift_cluster()}")
