"""Authored acceptance assertions for two REAL independent hosted invocations.

UNRUN. This does not synthesize a receipt, spawn a replacement, set Native facts,
or turn parsed JSON into authority. The operator supplies the actual frozen
result paths/hashes after each separate VM/process run. Kernel/source/build and
artifact provenance still require independent acceptance outside these checks.
"""
import hashlib
import json
import os
from pathlib import Path
import unittest


class ActualHostedOwnerEvidence(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        value = os.environ.get("LATTICE_ACTUAL_HOSTED_OWNER_EVIDENCE_MANIFEST")
        if not value:
            raise unittest.SkipTest("two actual fresh-process hosted evidence suppliers required")
        path = Path(value)
        cls.inputs = json.loads(path.read_text())
        assert set(cls.inputs) == {"history-lifecycle", "history-page-pair-failure"}

    def actual(self, case):
        item = self.inputs[case]
        path = Path(item["path"])
        self.assertEqual(path.resolve(strict=True), path)
        self.assertFalse(path.is_symlink())
        raw = path.read_bytes()
        self.assertLessEqual(len(raw), 16 * 1024 * 1024)
        self.assertEqual(hashlib.sha256(raw).hexdigest(), item["sha256"])
        result = json.loads(raw)
        self.assertEqual(result["sourceGraphSealSHA256"], item["sourceGraphSealSHA256"])
        self.assertEqual(result["harnessWorkflowCommit"], item["harnessWorkflowCommit"])
        self.assertEqual(result["scope"], "current-owner-invocation-and-test-os-services-only")
        self.assertFalse(result["runtimeAuthorityFromReceipts"])
        self.assertFalse(result["productGraphCommitInvented"])
        self.assertFalse(result["wholeOwnerCollectionQualified"])
        self.assertFalse(result["wholeCohortRetirementQualified"])
        self.assertFalse(result["historical13CasesQualified"])
        self.assertFalse(result["fixedLegacyFourRolesQualified"])
        self.assertFalse(result["providerLifecycleQualified"])
        self.assertFalse(result["accountDeletionUsedAsClosureProof"])
        self.assertFalse(result["unknownSessionEscapesProvenAbsent"])
        self.assertEqual(result["originalDeadlineSeconds"], 12600)
        account = result["account"]
        self.assertTrue(60000 <= account["uid"] < 80000)
        self.assertTrue(account["name"].startswith("lattice-owner-"))
        self.assertEqual(account["home"], "/Users/" + account["name"])
        self.assertTrue(account["retainedUntilVMTeardown"])
        build = result["actualBuildReceipt"]
        self.assertEqual(build["uid"], account["uid"])
        self.assertEqual(build["sourceGraphSealSHA256"], result["sourceGraphSealSHA256"])
        self.assertEqual(build["qualificationCaseData"], case)
        self.assertEqual(len(build["sourceRevision"]), 40)  # Actual builder observes it, no supplied product commit.
        for boundary in ["after-owner-images", "before-provider", "before-normal-owner-invocation", "after-normal-owner-invocation"]:
            self.assertIn(boundary, result["buildSourceChecks"])
        self.assertEqual(set(result["negativeOwnerPreflights"]), {"root", "dedicated"})
        for negative in result["negativeOwnerPreflights"].values():
            self.assertTrue(negative["qualified"])
            self.assertTrue(negative["ownerFailed"])
            self.assertTrue(negative["actualCommands"])
            self.assertTrue(any(row["exitCode"] != 0 for row in negative["actualCommands"]))
        self.assertTrue(result["directInvocationCustodyQualified"])
        for row in result["rootCommands"] + result["productCommands"]:
            self.assertTrue(row["leaderReaped"] and row["groupGone"] and row["success"])
            self.assertTrue(row["exitObserverRegistered"] and row["execGateReleased"])
            self.assertEqual(row["positiveCustodyScope"], "direct-leader-and-same-group-only")
            self.assertFalse(row["unknownSessionEscapesProvenAbsent"])
        outcome = result["controllerFirstOutcome"]
        self.assertTrue(outcome["ownerObservation"]["passed"])
        self.assertFalse(outcome["wholeCohortRetirementQualified"])
        self.assertFalse(outcome["ownerObservation"]["wholeOwnerCollectionQualified"])
        # Inner nonzero/debt can never become global qualification from the
        # Python leader's zero return or a passing per-family Native oracle.
        if outcome["controllerExit"] != 0:
            self.assertFalse(result["qualified"])
            self.assertFalse(result["controllerExitedZero"])
        return result

    def test_actual_history_lifecycle(self):
        result = self.actual("history-lifecycle")
        self.assertEqual(result["controllerFirstOutcome"]["kind"], "owner-history-payload-lifecycle")

    def test_actual_page_pair_injection_preserves_first_error(self):
        result = self.actual("history-page-pair-failure")
        outcome = result["controllerFirstOutcome"]
        self.assertEqual(outcome["kind"], "owner-history-page-pair-injected-failure")
        native = outcome["ownerObservation"]["native"]
        for snapshot in [native["beforeClose"], native["afterClose"]]:
            # Same real saved first Native exception, not requested intent.
            self.assertNotEqual(snapshot["flags"] & (1 << 7), 0)
            self.assertNotEqual(snapshot["flags"] & (1 << 8), 0)


if __name__ == "__main__":
    unittest.main()
