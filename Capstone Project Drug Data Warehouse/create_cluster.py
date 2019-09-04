import boto3
import configparser

def get_cluster_params(param_file):
    ''' Extract and return cluster parameters from configuration file.
    
    Args:
    config: the configuration parser
    param_file: filename of configuration file (.cfg)

    Returns: KEY, SECRET, CLUSTER_TYPE, NUM_NODES, NODE_TYPE, CLUSTER_IDENTIFIER, DB_NAME, DB_USER, DB_PASSWORD, DWH_PORT, IAM_ROLE_NAME
    '''
    config = configparser.ConfigParser()
    config.read_file(open(param_file))

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')
    CLUSTER_TYPE       = config.get("HW","CLUSTER_TYPE")
    NUM_NODES          = config.get("HW","NUM_NODES")
    NODE_TYPE          = config.get("HW","NODE_TYPE")
    CLUSTER_IDENTIFIER = config.get("ACCESS","CLUSTER_IDENTIFIER")
    DB_NAME                 = config.get("ACCESS","DB_NAME")
    DB_USER            = config.get("ACCESS","DB_USER")
    DB_PASSWORD        = config.get("ACCESS","DB_PASSWORD")
    DB_PORT               = config.get("ACCESS","DB_PORT")
    IAM_ROLE_NAME      = config.get("ACCESS", "IAM_ROLE_NAME")
    
    return(KEY, SECRET, CLUSTER_TYPE, NUM_NODES, NODE_TYPE, CLUSTER_IDENTIFIER, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT, IAM_ROLE_NAME)
    

def get_ARN(iam_role_name, key, secret):
    '''Retrieve ARN of existing IAM role to enable S3 read-only access from Redshift
    
    Args:
    iam_role_name: Name of existing IAM role with S3 read-only access policy
    key: Access Key ID for programmatic access to AWS API
    secret: Secret Access Key for programmatic access to AWS API
    
    Returns: ARN 
    '''   
    iam = boto3.client('iam', 
                       region_name='us-west-2', 
                       aws_access_key_id=key, 
                       aws_secret_access_key=secret)
    try:
        return iam.get_role(RoleName=iam_role_name)['Role']['Arn']
        
    except Exception:
        return None


def create_redshift_cluster(key, secret, cluster_type, node_type, num_nodes, db_name, cluster_identifier, db_user, db_password, roleArn):
    '''Create AWS redshift cluster via API
    
    Args:
    key: AWS API access ID key
    secret: AWS API secret key 
    cluster_type: 'single-node' or 'multi-node' 
    node_type: node type, e.g. dc2.large 
    num_nodes: number of nodes to create 
    db_name: database name 
    cluster_identifier: cluster name 
    db_user: database user name
    db_password: database user password
    roleArn: Amazon Resource Name (ARN) for IAM role
       
    Returns: Status message indicating success or failure to initiate cluster
    '''    
    redshift = boto3.client('redshift', 
                            region_name='us-west-2', 
                            aws_access_key_id=key, 
                            aws_secret_access_key=secret)
    
    try:
        response = redshift.create_cluster(        
            # add parameters for hardware
            ClusterType=cluster_type,
            NodeType=node_type,
            NumberOfNodes=int(num_nodes),

            # add parameters for identifiers & credentials
            DBName=db_name,
            ClusterIdentifier=cluster_identifier,
            MasterUsername=db_user,
            MasterUserPassword=db_password,

            # add parameter for role (to allow s3 access)
            IamRoles=[roleArn]         
        )

        return 'Creating cluster. Check management console for status.'
    
    except Exception as e:
        return ('Cluster creation failed: ' + str(e))


def main():
    key, secret, cluster_type, num_nodes, node_type, cluster_identifier, \
    db_name, db_user, db_password, db_port, iam_role_name = get_cluster_params('capstone.cfg')
    
    ARN = get_ARN(iam_role_name, key, secret)
    
    response = create_redshift_cluster(key, secret, cluster_type, node_type, num_nodes, db_name, cluster_identifier, db_user, db_password, ARN)
    print(response)
    

if __name__ == "__main__":
    main()
    
    
    
    