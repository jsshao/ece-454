cat output_hadoop/* | sort > normalized_outputA.txt
cat output_spark/* | sort > normalized_outputB.txt
diff normalized_outputA.txt normalized_outputB.txt
