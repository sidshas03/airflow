#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Simple test script to verify DAG tag validation functionality.
"""
from __future__ import annotations

import os
import sys

# Add the task-sdk to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'task-sdk', 'src'))

from datetime import datetime

from airflow.sdk.definitions.dag import DAG


def test_tag_validation():
    """Test DAG tag validation."""
    print("Testing DAG tag validation...")

    # Test 1: Tag with exactly 100 characters should be allowed
    try:
        dag = DAG(
            dag_id="test_dag_100",
            start_date=datetime(2021, 1, 1),
            tags=["a" * 100]
        )
        print("✓ Test 1 passed: 100-character tag allowed")
    except Exception as e:
        print(f"✗ Test 1 failed: {e}")

    # Test 2: Tag with 101 characters should raise ValueError
    try:
        dag = DAG(
            dag_id="test_dag_101",
            start_date=datetime(2021, 1, 1),
            tags=["a" * 101]
        )
        print("✗ Test 2 failed: 101-character tag should have been rejected")
    except ValueError as e:
        error_msg = str(e)
        if "101 characters long" in error_msg and "maximum limit of 100 characters" in error_msg:
            print("✓ Test 2 passed: 101-character tag correctly rejected")
        else:
            print(f"✗ Test 2 failed: Unexpected error message: {error_msg}")
    except Exception as e:
        print(f"✗ Test 2 failed: Unexpected exception: {e}")

    # Test 3: Multiple tags with one too long
    try:
        dag = DAG(
            dag_id="test_dag_multiple",
            start_date=datetime(2021, 1, 1),
            tags=["short", "a" * 101, "another_short"]
        )
        print("✗ Test 3 failed: Multiple tags with one too long should have been rejected")
    except ValueError as e:
        error_msg = str(e)
        if "101 characters long" in error_msg and "maximum limit of 100 characters" in error_msg:
            print("✓ Test 3 passed: Multiple tags with one too long correctly rejected")
        else:
            print(f"✗ Test 3 failed: Unexpected error message: {error_msg}")
    except Exception as e:
        print(f"✗ Test 3 failed: Unexpected exception: {e}")

    # Test 4: Very long tag should be trimmed in error message
    try:
        long_tag = "a" * 200
        dag = DAG(
            dag_id="test_dag_long",
            start_date=datetime(2021, 1, 1),
            tags=[long_tag]
        )
        print("✗ Test 4 failed: Very long tag should have been rejected")
    except ValueError as e:
        error_msg = str(e)
        if "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa..." in error_msg and "200 characters long" in error_msg:
            print("✓ Test 4 passed: Very long tag correctly rejected with trimmed preview")
        else:
            print(f"✗ Test 4 failed: Unexpected error message: {error_msg}")
    except Exception as e:
        print(f"✗ Test 4 failed: Unexpected exception: {e}")

if __name__ == "__main__":
    test_tag_validation()
