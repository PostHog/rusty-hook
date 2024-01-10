-- If running worker in transactional mode, this ensures we clean up any open transactions.
SET idle_in_transaction_session_timeout='2min';
