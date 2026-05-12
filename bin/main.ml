module FixedWindow : (Types.TIME_WINDOW with type window_identifier = int) = struct
    type window_identifier = int
    let int2window_identifier ts = ts / 10
    let is_expired current_win ts = (ts / 10) > current_win
end


module LogarithmicWindow : (Types.TIME_WINDOW with type window_identifier = int) = struct
    type window_identifier = int
    let log2 n = if n <= 0 then 0 else int_of_float (log (float_of_int n) /. log 2.0)
    let int2window_identifier ts = log2 (ts + 1)
    let is_expired current_win ts = log2 (ts + 1) > current_win
end


module AverageAggregator :
  (Types.AGGREGATOR
    with type input = float
     and type accumulator_state = float * int
     and type output = float) = struct
    type input = float
    type accumulator_state = float * int
    type output = float
    let init () = (0.0, 0)
    let ingest (sum, count) value = (sum +. value, count + 1)
    let finalize (sum, count) = if count = 0 then 0.0 else sum /. float_of_int count
end


module MinMaxAggregator :
  (Types.AGGREGATOR
    with type input = float
     and type accumulator_state = float * float
     and type output = float * float) = struct
    type input = float
    type accumulator_state = float * float
    type output = float * float
    let init () = (Float.max_float, -.Float.max_float)
    let ingest (c_min, c_max) value = (min c_min value, max c_max value)
    let finalize state = state
end


module VolumeAggregator :
  (Types.AGGREGATOR
    with type input = int
     and type accumulator_state = int
     and type output = int) = struct
    type input = int
    type accumulator_state = int
    type output = int
    let init () = 0
    let ingest acc value = acc + value
    let finalize acc = acc
end


module FixedAvgEngine =
  (Engine.MakeMetricsEngine(FixedWindow)(AverageAggregator)
   : Types.EngineOutput
     with type input_value = AverageAggregator.input
      and type output_summary = AverageAggregator.output
      and type window_id = FixedWindow.window_identifier)

module FixedMinMaxEngine =
  (Engine.MakeMetricsEngine(FixedWindow)(MinMaxAggregator)
   : Types.EngineOutput
     with type input_value = MinMaxAggregator.input
      and type output_summary = MinMaxAggregator.output
      and type window_id = FixedWindow.window_identifier)

module LogVolEngine =
  (Engine.MakeMetricsEngine(LogarithmicWindow)(VolumeAggregator)
   : Types.EngineOutput
     with type input_value = VolumeAggregator.input
      and type output_summary = VolumeAggregator.output
      and type window_id = LogarithmicWindow.window_identifier)


let run_avg_sim () =
    let data = [(2, 10.0); (5, 20.0); (12, 30.0); (15, 40.0); (25, 50.0)] in
    let state = List.fold_left (fun s (ts, v) -> FixedAvgEngine.ingest s ts v) (FixedAvgEngine.init ()) data in
    Printf.printf "--- Fixed Window (10s) + Average ---\n";
    List.iter (fun (id, res) ->
        Printf.printf "Bucket %d: Avg = %.2f\n" id res
    ) (FixedAvgEngine.history state)


let run_minmax_sim () =
    let data = [(1, 5.5); (4, 2.1); (11, 100.0); (18, 0.5); (22, 10.0)] in
    let state = List.fold_left (fun s (ts, v) -> FixedMinMaxEngine.ingest s ts v) (FixedMinMaxEngine.init ()) data in
    Printf.printf "\n--- Fixed Window (10s) + MinMax ---\n";
    List.iter (fun (id, (mi, ma)) ->
        Printf.printf "Bucket %d: Min = %.2f, Max = %.2f\n" id mi ma
    ) (FixedMinMaxEngine.history state)


let run_log_vol_sim () =
    let data = [(0, 10); (1, 20); (3, 30); (7, 40); (15, 50); (31, 60)] in
    let state = List.fold_left (fun s (ts, v) -> LogVolEngine.ingest s ts v) (LogVolEngine.init ()) data in
    Printf.printf "\n--- Logarithmic Window + Volume ---\n";
    List.iter (fun (id, vol) ->
        let s = (1 lsl id) - 1 in
        let e = (1 lsl (id + 1)) - 2 in
        Printf.printf "Bucket %d (Range %d-%d): Total Vol = %d\n" id s e vol
    ) (LogVolEngine.history state)


let () =
    run_avg_sim ();
    run_minmax_sim ();
    run_log_vol_sim ()
