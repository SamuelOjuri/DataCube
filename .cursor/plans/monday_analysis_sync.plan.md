<!-- e374ff8f-fe44-4954-a4df-c6a90a8f35b3 98f34209-60ae-4f13-b439-d0bfafa75adb -->
# Monday Analysis Sync Plan

1. Review Existing Analysis Outputs

- Confirm numeric fields created in `llm_analysis_all.py` and `AnalysisService.analyze_and_store`, noting schema of `final.predictions`.

2. Map Monday Column IDs

- Update `src/config.py` to add constants for `Datacube Gestation (Days)`, `Datacube Expected Conversion (%)`, and `Datacube Project Rating (x / 100)` column IDs on the parent board.

3. Extend Monday Client for Updates

- Add a helper to `src/core/monday_client.py` that wraps the `change_multiple_column_values` GraphQL mutation, handling retries and logging for column updates.
- Get results from Supabase `analysis_results` table for Monday fields:

`expected_gestation_days`  => `Datacube Gestation (Days)`

`expected_conversion_rate` => `Datacube Expected Conversion (%)`

`rating_score`             => `Datacube Project Rating (x / 100)`

`reasoning`                => `item's update`

Example mutation for `item's update`:

    ```
    mutation {
        create_update(
            item_id: 9876543210, 
            body: "This is the content of the update."
        ) {
            id
        }
        }
    ```

Example mutation to edit/update existing `item's update`:

    ```
    mutation {
        edit_update(
            id: 1234567890, 
            body: "The updated text!"
        ) {
            creator_id
            item_id
        }
        }
    ```

4. Implement Result→Monday Updater

- Create a small service (e.g., `src/services/monday_update_service.py`) that accepts project id plus gestation/conversion/rating, builds the column payload, and invokes the Monday client using `PARENT_BOARD_ID` and the new column IDs.

5. Integrate Updater Into Analysis Flows

- Wire the updater into `AnalysisService.analyze_and_store` (post Supabase write) and expose an opt-in flag in `scripts/llm_analysis_all.py` (CLI argument) to push each batch result back to Monday.

6. Add Logging & Throttling

- Ensure updates log successes/failures, respect existing rate-limit delays, and degrade gracefully (continue batch on individual failures).

7. Document Usage

- Update README or script docstring to describe the new `--update-monday` workflow and required env vars, plus note how to disable/enable the sync.

### To-dos

- [ ] Confirm analysis outputs expose expected gestation/conversion/rating values and data types.
- [ ] Add parent board column IDs for Datacube gestation, conversion, and rating fields in `src/config.py`.
- [ ] Implement Monday GraphQL mutation helper for numeric column updates.
- [ ] Build service to format and send analysis results to Monday columns, including logging and error handling.
- [ ] Invoke the updater from `AnalysisService` and optionally `llm_analysis_all.py` batches with throttling.
- [ ] Document the Monday update workflow and configuration requirements.