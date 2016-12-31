# Prevent Octave from thinking that this
# is a function file:

1;

# Function to compute a delta of a vector over a window.
#
# The window is in units of rows.  So if the rows are one-per-second,
# then a window of 60 means 1 minute.
function deltas = delta (counters, window)
  deltas = counters(1 + window : end) .- counters(1 : end - window)
endfunction

# Function to compute the rate of a matrix.
#
# The first column of the matrix is assumed to be timestamps in nanonsecs.
#
# The remaining columns are assumed to be cumulative counters.
function retval = rate (data, window)
  deltas = data(1 + window : end, 2 : end) .- data(1 : end - window, 2 : end)
  # Rely on implicit broadcast of timestamps in this division.
  rates = deltas ./ delta(data(:, 1), window) * 1e9
  retval = horzcat(data(1 + window : end, 1), rates)
endfunction

# Example usage:
#   plot_read_bytes_files("stats-20161230-190347.86.csv", "graph.png", 60)
function plot_read_bytes_files (filename, graph_filename, window)
  d = dlmread(filename, ",", 1, 0)
  rates = rate(d(:, [1, 7, 8]), window)
  plotyy(rates(:, 1), rates(:, 2), rates(:, 1), rates(:, 3))
  # axis([0, Inf, 0, Inf])
  print(graph_filename)
endfunction
