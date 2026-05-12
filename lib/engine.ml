module MakeMetricsEngine = functor (TW: Types.TIME_WINDOW) -> functor (A: Types.AGGREGATOR) -> struct
    type window_id = TW.window_identifier
    type input_value = A.input
    type output_summary = A.output

    type engine_state = {
        active_window : window_id option;
        active_accumulator : A.accumulator_state;
        historical_log : (window_id * output_summary) list;
    }

    let init () : engine_state = {
        active_window = None;
        active_accumulator = A.init ();
        historical_log = [];
    }

    let ingest state timestamp value =
        match state.active_window with
        | None ->
                let new_win = TW.int2window_identifier timestamp in
                let fresh_acc = A.init () in
                {
                    active_window = Some new_win;
                    active_accumulator = A.ingest fresh_acc value;
                    historical_log = state.historical_log;
                }
        | Some current_win ->
                if TW.is_expired current_win timestamp then
                    let finalized_summary = A.finalize state.active_accumulator in
                    let updated_history = (current_win, finalized_summary) :: state.historical_log in
                    let next_win = TW.int2window_identifier timestamp in
                    let fresh_acc = A.init () in
                    {
                        active_window = Some next_win;
                        active_accumulator = A.ingest fresh_acc value;
                        historical_log = updated_history;
                    }
                else
                    {
                        state with 
                        active_accumulator = A.ingest state.active_accumulator value;
                    }

    let history state = List.rev state.historical_log
end
