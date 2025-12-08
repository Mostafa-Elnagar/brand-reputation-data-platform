# EventBridge scheduling has been moved to 40_eventbridge module
# to resolve circular dependency between Lambda and Step Function.
#
# Deployment order:
# 1. 20_lambda (this module)
# 2. 30_stepfunction (references Lambda ARN)
# 3. 40_eventbridge (references Step Function ARN)
