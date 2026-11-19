# Pentaho pipeline (Pipeline A)

Files
- `Job.kjb` and `Merge_FINAL.ktr` are the original job and transformation.
- `Job_aligned.kjb` and `Transformation_aligned.ktr` add a Sort Rows step on Country and
  year before the Unique Rows step. This is the job timed in the paper.
- `Job_kosong.kjb` is an empty job (start and success only) used to measure JVM startup and
  Kitchen initialization.

All paths are relative to the file location (`${Internal.Entry.Current.Directory}`).
Input files are read from `../data/` and results are written to `../pentaho_output/`,
both at the repository root, so keep the repository layout as it is.

Run with Kitchen from the Pentaho installation folder

    kitchen.bat /file:"<path to repository>/pentaho/Job_aligned.kjb" /level:Basic

Tested with Pentaho CE 11.0.0.0-237. The original files from the student project used
`../Raw/` and `../Processed/`. Only these folder names were changed for this repository.

Note on the Unique Rows step. It is configured on Country and year, but in the original
job its input is not sorted, so it removes a record only when it repeats the key of the
record directly above it. See Section 4.1 of the paper and the `consecutive-key` mode in
`etl_pipeline.py`. The Merge Join steps join on country name, not on the ISO code.
