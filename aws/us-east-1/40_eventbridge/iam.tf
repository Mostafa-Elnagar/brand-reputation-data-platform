data "aws_iam_policy_document" "eventbridge_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "eventbridge_stepfunction_role" {
  name               = "${var.prefix}${var.name}-eventbridge-sfn-role"
  assume_role_policy = data.aws_iam_policy_document.eventbridge_assume_role.json

  tags = local.default_tags
}

data "aws_iam_policy_document" "eventbridge_start_execution" {
  statement {
    sid = "AllowStartStepFunctionExecution"

    actions = [
      "states:StartExecution",
    ]

    resources = [
      data.terraform_remote_state.stepfunction.outputs.state_machine_arn,
    ]
  }
}

resource "aws_iam_policy" "eventbridge_start_execution" {
  name   = "${var.prefix}${var.name}-eventbridge-sfn-policy"
  policy = data.aws_iam_policy_document.eventbridge_start_execution.json
}

resource "aws_iam_role_policy_attachment" "eventbridge_stepfunction_attach" {
  role       = aws_iam_role.eventbridge_stepfunction_role.name
  policy_arn = aws_iam_policy.eventbridge_start_execution.arn
}



