-- Snapshot actually tested during this run.
SELECT count(*) FROM lakehouse.demo.snapshot_demo FOR VERSION AS OF 6418069680668648572;
ALTER TABLE lakehouse.demo.snapshot_demo EXECUTE rollback_to_snapshot(6418069680668648572);
SELECT count(*) FROM lakehouse.demo.snapshot_demo;
