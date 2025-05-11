variable "dynamodb_table_lock_name" {
    description = "The name of your DynamoDB Lock Table"
    default = "lambda_locks_table"
}

variable "dynamodb_region" {
    description = "Supply the AWS region to create your DynamoDB Lock Table"
    default = "us-east-1"
}

variable "billing_mode" {
    description = "Billing Mode for your DynamoDB Lock Table - Accepted values are PROVISIONED or PAY_PER_REQUEST"
    default = "PAY_PER_REQUEST"
}

# This value is sufficient for 5 simultaneous executions of your application. Adjust this value according to 
# the concurrent executions of your application and monitor the performance in the monitoring graphs.
# If you start to see an increase in the Read throttled requests or Read throttled events graph, it is better to increase this value.
# variable "read_capacity" {
#     description = "The read capacity for your DynamoDB Lock Table. WORKS ONLY IN PROVISIONED BILLING MODE"
#     default = 5
#     type = number
# }

# This value is more than enough for a single lambda. Increase this value according to the number of different 
# locks that will be written to this table. A write_capacity value of 5 is sufficient for most applications.
# If you start to see an increase in the Write throttled requests or Write throttled events graph, it is better to increase this value.
# variable "write_capacity" {
#     description = "The write capacity for your DynamoDB Lock Table. WORKS ONLY IN PROVISIONED BILLING MODE"
#     default = 5
#     type = number
# }

variable "default_tags" {
    description = "Standard tags for your DynamoDB Lock Table"
    type        = map(any)
    default = {
        "Application"           = ""
        "Description"           = ""
        "Owner"                 = ""
    }
}

#################################################################################################################################

data "aws_caller_identity" "current" {}

provider "aws" {
    alias  = "ddblock"
    region = var.dynamodb_region
}

resource "aws_dynamodb_table" "locktable" {
    provider = aws.ddblock
    name             = var.dynamodb_table_lock_name
    billing_mode     = var.billing_mode
    hash_key         = "lock_id"
    # read_capacity    = var.read_capacity  # WORKS ONLY IN PROVISIONED BILLING MODE
    # write_capacity   = var.write_capacity # WORKS ONLY IN PROVISIONED BILLING MODE
    stream_enabled   = false

    attribute {
        name = "lock_id"
        type = "S" 
    }

    ttl {
        attribute_name = "ttl"
        enabled        = true
    }

    tags           = {
        Name   = "${var.default_tags}"
    }

    point_in_time_recovery {
        enabled = false
    }

    timeouts {}
}