module type TIME_WINDOW = sig
    type window_identifier
    
    val int2window_identifier: int -> window_identifier

    val is_expired: window_identifier -> int -> bool
end


module type AGGREGATOR = sig
    type input
    type accumulator_state
    type output

    val init: unit -> accumulator_state

    val ingest: accumulator_state -> input -> accumulator_state

    val finalize: accumulator_state -> output
end


module type EngineOutput = sig
    type window_id
    type input_value
    type output_summary
    type engine_state

    val init: unit -> engine_state

    val ingest: engine_state -> int -> input_value -> engine_state

    val history: engine_state -> (window_id * output_summary) list
end
