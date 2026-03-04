
echo "Running benchmarks..."
#echo "Publishing and subscribing with pure Python implementation (sequential- asyncio)..."
#python publish_sub_pure_sequential.py
echo "Publishing and subscribing with pure Python implementation (sequential- uvloop)..."
python uv_publish_sub_pure_sequential.py
echo "Publishing and subscribing with pure Python implementation (concurrent- uvloop)..."
python uv_publish_sub_pure_concurrent.py
#echo "Publishing and subscribing with pure Python implementation (parallel- uvloop)..."
#python uv_publish_sub_pure_parallel.py # if no-gil
#echo "Publishing and subscribing with Rust implementation (sequential- uvloop)..."
#python uv_publish_sub_rs_sequential.py
echo "Publishing and subscribing with Rust implementation (concurrent- uvloop)..."
python uv_publish_sub_rs_concurrent.py
echo "Publishing and subscribing with Rust implementation (parallel- uvloop)..."
python uv_publish_sub_rs_parallel.py
echo "Rpc with pure Python implementation (sequential- asyncio)..."
python rpc_client_server_pure_sequential.py
echo "Rpc with pure Rust implementation (sequential- asyncio)..."
python rpc_client_server_rs_sequential.py