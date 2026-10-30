-- One-time migration for analytics databases created before the Bronze/Silver/Gold rename.
-- Run this before the first Meltano sync after deploying the new configuration.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'raw') THEN
        IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'bronze') THEN
            RAISE EXCEPTION
                'Cannot rename raw to bronze because both schemas exist. Reconcile them first.';
        END IF;
        ALTER SCHEMA raw RENAME TO bronze;
    END IF;

    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'staging') THEN
        IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'silver') THEN
            RAISE EXCEPTION
                'Cannot rename staging to silver because both schemas exist. Reconcile them first.';
        END IF;
        ALTER SCHEMA staging RENAME TO silver;
    END IF;

    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'staging_core') THEN
        IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'gold') THEN
            RAISE EXCEPTION
                'Cannot rename staging_core to gold because both schemas exist. Reconcile them first.';
        END IF;
        ALTER SCHEMA staging_core RENAME TO gold;
    END IF;
END
$$;
