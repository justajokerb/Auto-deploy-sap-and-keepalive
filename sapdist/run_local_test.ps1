# Local integration test script for sapdist cluster

$adminPass = "testadmin123"
$clusterSecret = "testclustersecret456"

# Cleanup from previous runs
Write-Host "Cleaning up directories and metadata..." -ForegroundColor Cyan
Remove-Item -Path "storage1", "storage2", "metadata.json" -Recurse -ErrorAction SilentlyContinue

# Ensure clean directories
New-Item -ItemType Directory -Path "storage1" -Force | Out-Null
New-Item -ItemType Directory -Path "storage2" -Force | Out-Null

# 1. Start Master
Write-Host "Starting Master Controller on port 8080..." -ForegroundColor Cyan
$env:ADMIN_PASSWORD = $adminPass
$env:CLUSTER_SECRET = $clusterSecret
$masterProc = Start-Process go -ArgumentList "run master/master.go -port 8080 -meta metadata.json" -NoNewWindow -PassThru

# 2. Start Workers
Write-Host "Starting Worker 1 on port 8081..." -ForegroundColor Cyan
$worker1Proc = Start-Process go -ArgumentList "run worker/worker.go -port 8081 -dir ./storage1 -master http://localhost:8080 -id worker-1 -secret $clusterSecret" -NoNewWindow -PassThru

Write-Host "Starting Worker 2 on port 8082..." -ForegroundColor Cyan
$worker2Proc = Start-Process go -ArgumentList "run worker/worker.go -port 8082 -dir ./storage2 -master http://localhost:8080 -id worker-2 -secret $clusterSecret" -NoNewWindow -PassThru

# Wait for registration and heartbeats to settle
Write-Host "Waiting for workers to register..." -ForegroundColor Yellow
$headers = @{
    Authorization = "Basic " + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("admin:$adminPass"))
}

$retries = 15
$registered = $false

try {
    for ($i = 1; $i -le $retries; $i++) {
        Start-Sleep -Seconds 1
        try {
            $status = Invoke-RestMethod -Uri "http://localhost:8080/api/status" -Headers $headers -Method Get
            $onlineCount = 0
            foreach ($node in $status.nodes) {
                if ($node.status -eq "online") {
                    $onlineCount++
                }
            }
            if ($onlineCount -ge 2) {
                Write-Host "Both workers registered and online!" -ForegroundColor Green
                $registered = $true
                break
            }
        } catch {
            # Master might still be starting up
        }
    }

    if (-not $registered) {
        throw "Workers failed to register online within timeout."
    }

    # Print registered nodes
    $status = Invoke-RestMethod -Uri "http://localhost:8080/api/status" -Headers $headers -Method Get
    Write-Host ("Total Pool Space: " + $status.total_space + " bytes") -ForegroundColor Green
    Write-Host ("Nodes Count: " + $status.nodes.Count) -ForegroundColor Green
    foreach ($node in $status.nodes) {
        Write-Host "Node: $($node.id), Status: $($node.status), Used: $($node.used_space) bytes" -ForegroundColor Gray
    }

    # 4. Create and upload a sample file
    Write-Host "Creating sample file for upload..." -ForegroundColor Cyan
    $sampleFile = "test_upload_file.txt"
    $fileContent = "Hello Sapdist! This is a secure distributed storage test file. " * 50
    Set-Content -Path $sampleFile -Value $fileContent
    $originalHash = (Get-FileHash $sampleFile).Hash

    Write-Host "Uploading file via chunked upload..." -ForegroundColor Cyan

    $filePath = Resolve-Path $sampleFile
    $fileBytes = [System.IO.File]::ReadAllBytes($filePath)
    $fileSize = $fileBytes.Length
    $chunkSize = 2000 # 2KB chunks for testing
    $totalChunks = [Math]::Max(1, [Math]::Ceiling($fileSize / $chunkSize))

    $authValue = "Basic " + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("admin:$adminPass"))
    $headersJson = @{
        Authorization = $authValue
        "Content-Type" = "application/json"
    }

    # Step 1: Start
    $startBody = @{
        filename = "test_upload_folder/nested/test_upload_file.txt"
        size = $fileSize
        totalChunks = $totalChunks
    } | ConvertTo-Json

    $startResp = Invoke-RestMethod -Uri "http://localhost:8080/api/upload/start" -Headers $headersJson -Method Post -Body $startBody
    $uploadId = $startResp.uploadId
    Write-Host "Upload started, Session ID: $uploadId" -ForegroundColor Gray

    # Step 2: Upload Chunks
    $enc = [System.Text.Encoding]::GetEncoding("iso-8859-1")
    for ($i = 0; $i -lt $totalChunks; $i++) {
        $startByte = $i * $chunkSize
        $endByte = [Math]::Min($fileSize, $startByte + $chunkSize)
        $len = $endByte - $startByte

        $chunkBytes = New-Object byte[] $len
        [Array]::Copy($fileBytes, $startByte, $chunkBytes, 0, $len)
        $chunkStr = $enc.GetString($chunkBytes)

        $boundary = [System.Guid]::NewGuid().ToString()
        $LF = "`r`n"
        $bodyLines = (
            "--$boundary",
            "Content-Disposition: form-data; name=`"uploadId`"",
            "",
            $uploadId,
            "--$boundary",
            "Content-Disposition: form-data; name=`"chunkIndex`"",
            "",
            $i.ToString(),
            "--$boundary",
            "Content-Disposition: form-data; name=`"file`"; filename=`"chunk`"",
            "Content-Type: application/octet-stream",
            "",
            $chunkStr,
            "--$boundary--"
        ) -join $LF

        $chunkHeaders = @{
            Authorization = $authValue
            "Content-Type" = "multipart/form-data; boundary=$boundary"
        }

        $chunkResp = Invoke-WebRequest -Uri "http://localhost:8080/api/upload/chunk" -Headers $chunkHeaders -Method Post -Body $bodyLines
        Write-Host "Chunk $i uploaded, Status: $($chunkResp.StatusCode)" -ForegroundColor Gray
    }

    # Step 3: Finish
    $finishBody = @{
        uploadId = $uploadId
    } | ConvertTo-Json

    $finishResp = Invoke-WebRequest -Uri "http://localhost:8080/api/upload/finish" -Headers $headersJson -Method Post -Body $finishBody
    Write-Host "Upload Finished, Status: $($finishResp.StatusCode) (Created)" -ForegroundColor Green

    # Wait for status sync
    Start-Sleep -Seconds 2

    # 5. Check replication (chunk should exist in both storage1 and storage2)
    Write-Host "Verifying RAID-1 replication in storage directories..." -ForegroundColor Cyan
    $storage1Files = Get-ChildItem -Path "storage1"
    $storage2Files = Get-ChildItem -Path "storage2"
    Write-Host "Chunks in Storage 1: $($storage1Files.Count)" -ForegroundColor Green
    Write-Host "Chunks in Storage 2: $($storage2Files.Count)" -ForegroundColor Green

    if ($storage1Files.Count -gt 0 -and $storage2Files.Count -gt 0) {
        Write-Host "Replication Verified: Chunk was successfully copied to both workers!" -ForegroundColor Green
    } else {
        Write-Warning "Replication Failed: Chunk not present on both nodes."
    }

    # 6. Test File download and hash comparison
    Write-Host "Testing download..." -ForegroundColor Cyan
    $downloadHeaders = @{
        Authorization = "Basic " + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("admin:$adminPass"))
    }
    $downloadedFile = "test_downloaded.txt"
    Invoke-RestMethod -Uri "http://localhost:8080/api/download/test_upload_folder/nested/test_upload_file.txt" -Headers $downloadHeaders -OutFile $downloadedFile

    $downloadedHash = (Get-FileHash $downloadedFile).Hash
    Write-Host "Original file hash:   $originalHash" -ForegroundColor Gray
    Write-Host "Downloaded file hash: $downloadedHash" -ForegroundColor Gray

    if ($originalHash -eq $downloadedHash) {
        Write-Host "Success: File integrity verified!" -ForegroundColor Green
    } else {
        Write-Error "Error: File integrity verification failed!"
    }

    # Clean up test files
    Remove-Item $sampleFile -ErrorAction SilentlyContinue
    Remove-Item $downloadedFile -ErrorAction SilentlyContinue

} catch {
    Write-Error "Test encountered error: $_"
} finally {
    # Stop background processes
    Write-Host "Stopping Master and Worker processes..." -ForegroundColor Yellow
    Stop-Process -Id $masterProc.Id -Force -ErrorAction SilentlyContinue
    Stop-Process -Id $worker1Proc.Id -Force -ErrorAction SilentlyContinue
    Stop-Process -Id $worker2Proc.Id -Force -ErrorAction SilentlyContinue
}
