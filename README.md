# omar-scdf-aggregator

Receives a message from a omar-sqs-notifier.  Checks the given S3 bucket for the configured files, and drops the message, or send an aggregate message to the next SCDF application in the chain.
