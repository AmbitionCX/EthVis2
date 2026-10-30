# Legacy v0.2 reference

This package and its existing tests are preserved during the v0.3 transition.
The CLI no longer imports it. Do not add new collection features here.
The active implementation lives in app/config/domain/beacon/archive/decode/
pipeline/storage. Legacy reward integration tests additionally require the legacy build tag and
explicit test variables. They are excluded from the normal v0.3 test run.
