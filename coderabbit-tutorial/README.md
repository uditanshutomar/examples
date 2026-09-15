# Diagnosing Failing Integration Tests with CodeRabbit and Signadot

This example runs HotROD's Playwright tests in a Signadot sandbox for every pull request. When a change
breaks another service, the failing check appears on the pull request, where CodeRabbit can explain it
and propose a repair that runs through the same tests.

For the guided step-by-step walkthrough, see the
[full tutorial](https://www.signadot.com/docs/tutorials/coderabbit-signadot-failing-tests).

## What's in this directory

The `files` directory mirrors the root of your HotROD copy. The tutorial copies it in with
`cp -R ../signadot-examples/coderabbit-tutorial/files/. .`. Everything here was tested with HotROD
revision `25b56d7a7494b5afac52f2def76f897bedfa76eb`.

| File | Purpose |
| --- | --- |
| `.github/workflows/signadot-e2e.yml` | Builds the driver image, creates the pull request's sandbox, and runs the Playwright Job |
| `.signadot/sandbox.yaml` | Sandbox template that forks the `driver` Deployment |
| `.signadot/testing/runner.yaml` | Runner Group that runs Jobs in your cluster |
| `.signadot/testing/job.yaml` | Job that checks out the pull request's commit and runs the Playwright tests |
| `.signadot/scripts/ci-inputs.sh` | Checks the workflow inputs and derives the sandbox and image names |
| `.signadot/scripts/build.sh` | Builds HotROD's frontend assets, runs Go tests and vet, and compiles the binary |
| `.coderabbit.yaml` | CodeRabbit review settings and path instructions |
| `tutorial/break-dispatch.patch` | The regression, which renames the dispatch JSON fields and adds a pickup guard |
| `tutorial/reference-repair.patch` | A repair CodeRabbit generated for the regression, to use in place of Fix CI |
| `tutorial/deployed_payload_test.go.txt` | Contract test that decodes the message the deployed `frontend` sends |
| `tutorial/preserve-wire.patch` | Optional refactor that keeps the JSON field names |

## Values the workflow reads

Set these on your tutorial repository with `gh variable set` and `gh secret set`, as the tutorial shows.

| Name | Kind | Example | Notes |
| --- | --- | --- | --- |
| `SIGNADOT_CLUSTER` | Variable | `my-cluster` | Cluster name from `signadot cluster list` |
| `HOTROD_NAMESPACE` | Variable | `hotrod` | Namespace that runs the baseline `frontend` and `driver` |
| `HOTROD_ARCH` | Variable | `amd64` | `amd64` or `arm64`, matching the nodes that run `driver` |
| `SIGNADOT_RUNNER_GROUP` | Variable | `hotrod-coderabbit` | An unused name of up to 30 lowercase letters, digits or hyphens, starting with a letter and ending with a letter or digit. Pick it before you set this variable. |
| `SIGNADOT_ORG` | Secret | `my-org` | Organization from `signadot auth status` |
| `SIGNADOT_API_KEY` | Secret | | API key from a service account with the `member` role |

The workflow uses shorter names for some of these values. A validation error that mentions `CLUSTER`,
`RUNNER_GROUP` or `TARGET_ARCH` refers to `SIGNADOT_CLUSTER`, `SIGNADOT_RUNNER_GROUP` or `HOTROD_ARCH`.

## Troubleshooting

| What you see | What to check |
| --- | --- |
| The push is rejected for `.github/workflows/signadot-e2e.yml` | Run `gh auth refresh --hostname github.com --scopes workflow`, then push again. |
| No workflow run on the pull request | Open the pull request from a branch in the same repository. The workflow skips pull requests from forked repositories. |
| **Validate inputs and derive names** fails | This step checks the repository variables, so compare them with the table above. If it exits without a message, look for an unsupported `HOTROD_ARCH`, a Runner Group name longer than 30 characters, or a namespace longer than 63 characters. |
| **Create or update the PR sandbox** says `failed to find pull request` | In GitHub **Settings > Applications > Installed GitHub Apps > Configure**, confirm the Signadot App's saved repository selection includes your tutorial repository. Check that your Signadot organization is linked to that installation, save any change, then rerun the failed job. |
| **Create or update the PR sandbox** reports an authentication error | Check `SIGNADOT_ORG` and `SIGNADOT_API_KEY`, and whether the key has expired. |
| The sandbox readiness wait times out | Run `kubectl --context "$TEST_CONTEXT" -n "$HOTROD_NAMESPACE" get pods` and look for `ImagePullBackOff`. Check the `hotrod-ghcr-read` Secret, its namespace, and `HOTROD_ARCH`. |
| The Job stays queued | Run `signadot jrg get "$SIGNADOT_RUNNER_GROUP"` and `kubectl --context "$TEST_CONTEXT" -n signadot-tests get pods,events`. The runner has one pod, so Jobs run one at a time. |
| The Job fails before the tests start | For a private repository, check the deploy key and the `hotrod-git-read` Secret. For a public repository, make sure this runner mounts no key from another repository. If the Secret name is taken, pick a new name in `runner.yaml` rather than deleting a shared credential. |
| The control fails, or the regression passes | Compare `PR_HEAD` with the pull request's latest commit, and check that the baseline `frontend` and `driver` are running. |
| No Signadot comment on the pull request | Check the Signadot GitHub App's access to the repository and the two `signadot/github-*` labels in the sandbox template. |
| No CodeRabbit review | Confirm CodeRabbit's saved repository selection includes your new repository, then check your plan. Pull request reviews on a private repository need a paid plan or a trial, because the Free plan gives pull request summaries only. On [public repositories with fewer than 10 stars](https://docs.coderabbit.ai/management/plans#rate-limits), comment `@coderabbitai review` to start one. |
| `@coderabbitai fix-ci` does nothing | [Fix CI requires Team, Advanced or Enterprise](https://docs.coderabbit.ai/management/plans#feature-availability), including an eligible trial. Without it, use `tutorial/reference-repair.patch` as the tutorial describes. |
| The contract test fails on the repair | Make sure you checked out the repair branch, then look at how the repair decodes the camelCase fields. |

## Private repositories

Jobs fetch the pull request's commit from inside your cluster, over HTTPS for a public repository. For a
private repository, create the read-only deploy key and the `hotrod-git-read` Secret from the tutorial
before you create the Runner Group. Any code that runs in a Job can read that Secret once the Runner
Group mounts it, so use this runner only for the tutorial repository.

## Run times and timeouts

A run takes about five minutes, build and test Job together. With one runner pod, Jobs from different
pull requests run one at a time. Each Job times out after 15 minutes (`jobTimeout` in `runner.yaml`),
and the workflow job after 40 minutes, including sandbox startup and time in the queue. The CLI checks
`--timeout 25m` only between polls, so it won't stop a log stream that is already running.

## Cleanup

The tutorial's cleanup section closes the pull requests and removes the Runner Group and credentials
created for this exercise, while keeping the namespace and any other workloads in it. After you close
the pull requests, run `signadot sandbox list` to check that the GitHub App removed their sandboxes. If
a tutorial sandbox is still listed a few minutes later, find its name in the pull request's Signadot
comment and delete only that sandbox with `signadot sandbox delete NAME`. Close `demo/preserve-wire` too
if you tried the optional refactor.

Revoke the keys you created for the tutorial. If you no longer need the image, delete the GHCR package
separately. Delete the service account only if you created it for this tutorial and nothing else uses
it.
