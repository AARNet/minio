#
# Helpers
#

function timestamp_echo {
  echo "[$(date +'%Y-%m-%d %R')] ${@}"
}

function echo_info {
  timestamp_echo "[INFO ] ${@}"
}

function echo_error {
  timestamp_echo "[ERROR] ${@}"
}

function test_run {
  cmd="${1}"
  message="${2:-${cmd}}"
  reverse="${@: -1}"
  ${cmd} > /dev/null
  if [[ $? -ne 0 ]]; then
    if [ "$reverse" == "0" ]; then
      echo_error "FAIL: ${message}"
      return 1
    else
      echo_info "PASS (read-only): ${message}"
      return 0
    fi
  else
    if [ "$reverse" == "0" ]; then
      echo_info "PASS: ${message}"
      return 0
    else
      echo_error "FAIL (read-only): ${message}"
      return 1
    fi
  fi
}

function exit_on_failure {
  result="${1}"
  message="${2:-unknown error}"
  if [ "${result}" == "" ]; then
     echo_error "Unknown result"
     return 1
  fi
  if [ ${result} -ne 0 ]; then
    echo_error "${message}"
    return 1
  fi
  return 0
}

#
# Tests
#

function test_mc_cp {
  local mc="${1}"
  local src="${2}"
  local dest="${3}"
  local reverse="${4}"
  test_file_upload=$(${mc} --json cp "${src}" "${dest}")
  if [ $? -ne 0 ] && [ "${reverse}" == "0" ]; then
    echo_error "FAIL:  ${mc} cp ${src} ${dest}"
    return 1
  elif [ $? -ne 0 ] && [ "${reverse}" == "1" ]; then
    speed=$(echo ${test_file_upload} | jq -j 'select(.speed) | (.speed/1024/1024)*100 | floor/100' || echo 'unknown')
    size=$(echo ${test_file_upload} | jq -j 'select(.size) | .size' || echo 'unknown')
    echo_info "PASS (read-only): ${mc} cp ${src} ${dest} (${size}B at ${speed} MB/s)"
    return 0
  elif [ $? -eq 0 ] && [ "${reverse}" == "0" ]; then
    speed=$(echo ${test_file_upload} | jq -j 'select(.speed) | (.speed/1024/1024)*100 | floor/100' || echo 'unknown')
    size=$(echo ${test_file_upload} | jq -j 'select(.size) | .size' || echo 'unknown')
    echo_info "PASS: ${mc} cp ${src} ${dest} (${size}B at ${speed} MB/s)"
    return 0
  elif [ $? -eq 0 ] && [ "${reverse}" == "1" ]; then
    echo_error "FAIL (read-only):  ${mc} cp ${src} ${dest}"
    return 1
  fi
}

function test_mc_fileinfo {
  local mc="${1}"
  local src="${2}"
  local expected_size=${3}
  local expected_key=${4}
  local expected_etag=${5}
  local reverse=${6}
  local fileinfo=$(${mc} --json ls ${src})
  if [ $? -eq 0 ] && [ "${reverse}" == "0" ]; then
    key=$(echo ${fileinfo} | jq -j '.key' || echo "")
    if [ "${key}" != "${expected_key}" ]; then
      echo_error "FAIL: Key of uploaded '${src}' is not correct [expected=${expected_key}, got=${key}]"
      return 1
    fi
    size=$(echo ${fileinfo} | jq -j '.size' || echo "")
    if [ "${size}" -ne "${expected_size}" ]; then
      echo_error "FAIL: Size of uploaded '${src}' file size is not correct [expected=${expected_size}, got=${size}]"
      return 1
    fi
    etag=$(echo ${fileinfo} | jq -j '.etag' || echo "")
    if [ "${etag}" != "${expected_etag}" ]; then
      echo_error "FAIL: etag of uploaded '${src}' is not correct [expected=${expected_etag}, got=${etag}]"
      return 1
    fi
  elif [ $? -eq 0 ] && [ "${reverse}" == "1" ]; then
    echo_error "FAIL (read-only): file information successfully obtained from a read-only gateway"
    return 1
  elif [ $? -eq 1 ] && [ "${reverse}" == "1" ]; then
    return 0
  elif [ $? -eq 1 ] && [ "${reverse}" == "0" ]; then
    echo_error "FAIL: Unable to get file information for '${src}'"
    return 1
  fi
  return 0
}

function add_return_code {
  current=$1
  to_add=$2
  echo $(( ${current} + ${to_add} ))
  return ${to_add}
}

function test_mc {
  mc="/usr/bin/mc"
  local url="${1}"
  local access_key="${2}"
  local secret_key="${3}"
  local read_only="${4}"
  local tmp_string="$(< /dev/urandom tr -dc a-z | head -c5)"
  local test_gateway="minio"
  local test_bucket="testbucket-${tmp_string}"
  local test_file="test-${tmp_string}"
  local test_file_multipart="test-multipart-${tmp_string}"
  local test_file_multipart_plus="test+multipart-${tmp_string}"
  local test_result=0

  echo_info "Setting up minio client"
  ${mc} config host ls ${test_gateway} > /dev/null 2>&1
  if [ $? -eq 0 ]; then
    test_run "${mc} config host remove ${test_gateway}" "Removing existing config" 0
    test_result=$(add_return_code ${test_result} $?)
  fi
  test_run "${mc} config host add ${test_gateway} ${url} ${access_key} ${secret_key}" "${mc} config host add ${test_gateway} ${url} xxxxxx xxxxxxxxxxxxxx" 0
  test_result=$(add_return_code ${test_result} $?)
  rc=$?

  echo_info "Running tests using minio client"
  if [[ ${rc} -eq 0 ]]; then
    test_run "${mc} mb ${test_gateway}/${test_bucket}" "${mc} mb ${test_gateway}/${test_bucket}" $read_only
    test_result=$(add_return_code ${test_result} $?)
    if [[ $? -eq 0 ]]; then
      echo_info "Generating test files"
      echo "does it work?" > "${test_file}"
      dd if=/dev/urandom of="${test_file_multipart}" bs=1M count=128 status=none
      testfile_etag=$(cat "${test_file}" | md5sum | awk '{ print $1 }')
      testfilexrdcp_etag=$(cat "${test_file_multipart}" | md5sum | awk '{ print $1 }')

      # Test that we can upload into a bucket
      test_mc_cp "${mc}" "${test_file}" "${test_gateway}/${test_bucket}/${test_file}" $read_only
      test_result=$(add_return_code ${test_result} $?)
      file_copied=$?
      if [ $file_copied -eq 0 ]; then
        test_mc_fileinfo "${mc}" "${test_gateway}/${test_bucket}/${test_file}" 14 "${test_file}" "$testfile_etag" $read_only
        test_result=$(add_return_code ${test_result} $?)
      fi

      # Test that we can upload into a directory in a bucket
      test_mc_cp "${mc}" "${test_file}" "${test_gateway}/${test_bucket}/inadirectory/${test_file}"
      test_result=$(add_return_code ${test_result} $?)
      file_copied=$?
      if [ $file_copied -eq 0 ]; then
        test_mc_fileinfo "${mc}" "${test_gateway}/${test_bucket}/inadirectory/${test_file}" 14 "${test_file}" "${testfile_etag}"
        test_result=$(add_return_code ${test_result} $?)
      fi

      # Test upload "big" file (xrdcp)
      test_mc_cp "${mc}" "${test_file_multipart}" "${test_gateway}/${test_bucket}/${test_file_multipart}"
      test_result=$(add_return_code ${test_result} $?)
      file_copied=$?
      if [ $file_copied -eq 0 ]; then
        echo_info "Waiting 5 seconds to allow multipart transfer to complete"
        sleep 5
        test_mc_fileinfo "${mc}" "${test_gateway}/${test_bucket}/${test_file_multipart}" 134217728 "${test_file_multipart}" "${testfilexrdcp_etag}"
        test_result=$(add_return_code ${test_result} $?)
        test_run "${mc} rm ${test_gateway}/${test_bucket}/${test_file_multipart}" "${mc} rm ${test_gateway}/${test_bucket}/${test_file_multipart}" $read_only
        test_result=$(add_return_code ${test_result} $?)
      fi

      # Test upload "big" file with a '+' in the name (xrdcp)
      test_mc_cp "${mc}" "${test_file_multipart}" "${test_gateway}/${test_bucket}/${test_file_multipart_plus}"
      test_result=$(add_return_code ${test_result} $?)
      file_copied=$?
      if [ $file_copied -eq 0 ]; then
        echo_info "Waiting 5 seconds to allow multipart transfer to complete"
        sleep 5
        test_mc_fileinfo "${mc}" "${test_gateway}/${test_bucket}/${test_file_multipart_plus}" 134217728 "${test_file_multipart_plus}" "${testfilexrdcp_etag}"
        test_result=$(add_return_code ${test_result} $?)
        test_run "${mc} rm ${test_gateway}/${test_bucket}/${test_file_multipart_plus}" "${mc} rm ${test_gateway}/${test_bucket}/${test_file_multipart_plus}" $read_only
        test_result=$(add_return_code ${test_result} $?)
      fi

      test_mc_cp "${mc}" "${test_file_multipart}" "${test_gateway}/${test_bucket}/inadirectory-xrdcp/${test_file_multipart}"
      test_result=$(add_return_code ${test_result} $?)
      file_copied=$?
      if [ $file_copied -eq 0 ]; then
        echo_info "Waiting 5 seconds to allow multipart transfer to complete"
        sleep 5
        test_mc_fileinfo "${mc}" "${test_gateway}/${test_bucket}/inadirectory-xrdcp/${test_file_multipart}" 134217728 "${test_file_multipart}" "${testfilexrdcp_etag}"
        test_result=$(add_return_code ${test_result} $?)
        test_run "${mc} rm ${test_gateway}/${test_bucket}/inadirectory-xrdcp/${test_file_multipart}" "${mc} rm ${test_gateway}/${test_bucket}/inadirectory-xrdcp/${test_file_multipart}"  $read_only
        test_result=$(add_return_code ${test_result} $?)
      fi

      test_run "${mc} ls --recursive ${test_gateway}/${test_bucket}" "${mc} ls --recursive ${test_gateway}/${test_bucket}" $read_only
      test_result=$(add_return_code ${test_result} $?)
      # Retrieve file
      if [[ $file_copied -eq 0 ]]; then
        test_mc_cp "${mc}" "${test_gateway}/${test_bucket}/${test_file}" "${test_file}-retrieved"
        test_result=$(add_return_code ${test_result} $?)
        if [[ $? -eq 0 ]]; then
          rm ${test_file}-retrieved
        fi
      fi

      # Remove file
      if [[ $file_copied -eq 0 ]]; then
        test_run "${mc} rm ${test_gateway}/${test_bucket}/test-${tmp_string}" "${mc} rm ${test_gateway}/${test_bucket}/test-${tmp_string}"  $read_only
        test_result=$(add_return_code ${test_result} $?)
      fi
      # List files
      test_run "${mc} ls ${test_gateway}/${test_bucket}" "${mc} ls ${test_gateway}/${test_bucket}" $read_only
      test_result=$(add_return_code ${test_result} $?)

      # Cleanup
      echo_info "Removing bucket recursively, this might take a minute."
      test_run "${mc} rb --force ${test_gateway}/${test_bucket}" "${mc} rb --force ${test_gateway}/${test_bucket}" $read_only
      test_result=$(add_return_code ${test_result} $?)
    fi
  fi

  if [[ -f "${test_file}" ]]; then
    rm "${test_file}"
  fi

  if [[ -f "${test_file_multipart}" ]]; then
    rm "${test_file_multipart}"
  fi

  if [ ${test_result} -gt 0 ]; then
    exit ${test_result}
  fi
}

# Check if gateway is read only
read_only=0
if [ "${EOSREADONLY}" = "true" ]; then
  read_only=1
fi

test_mc http://127.0.0.1:9000/ ${MINIO_ACCESS_KEY} ${MINIO_SECRET_KEY} ${read_only}
