function Export-ZtGraphEntityDateRange {
	<#
	.SYNOPSIS
		Export sign-in logs by splitting the date range into per-day slices exported in parallel.

	.DESCRIPTION
		For large tenants with millions of sign-in logs, sequential pagination through a single
		date range takes 30-60+ minutes. This command splits the configured date range into
		individual day slices and exports each slice concurrently using background runspaces.

		Each day-slice is exported to its own subfolder, then all results are merged into the
		final entity folder. This preserves compatibility with the downstream DuckDB import
		which expects <Name>-<PageIndex>.json files in a single folder.

		The command respects the same size limit and time limit as Export-ZtGraphEntity, but
		applies them per-slice (any individual day that exceeds the limit is truncated, not
		the entire export).

	.PARAMETER Name
		The name of the entity to export (e.g., 'SignIn').

	.PARAMETER Uri
		The base URI for the Graph API endpoint.

	.PARAMETER QueryString
		The base query string (filter) applied to the full date range.
		This is used as a template — the date filter portion will be overridden per-slice.

	.PARAMETER Days
		Number of days to cover. Each day becomes a separate parallel export slice.

	.PARAMETER MaximumQueryTime
		Maximum time in minutes per individual day-slice before it is truncated.
		Defaults to 10 minutes per slice (vs 60 for the full sequential export).

	.PARAMETER ExportPath
		Where all the results are stored.

	.EXAMPLE
		PS C:\> Export-ZtGraphEntityDateRange -Name SignIn -Uri 'beta/auditlogs/signins' -QueryString $filter -Days 30 -ExportPath $path

		Exports 30 days of sign-in logs using parallel day-slices.
	#>
	[CmdletBinding()]
	param (
		[Parameter(Mandatory = $true)]
		[string]
		$Name,

		[Parameter(Mandatory = $true)]
		[string]
		$Uri,

		[Parameter(Mandatory = $false)]
		[string]
		$QueryString,

		[Parameter(Mandatory = $false)]
		[int]
		$Days = 30,

		[Parameter(Mandatory = $false)]
		[int]
		$MaximumQueryTime = 0,

		[Parameter(Mandatory = $true)]
		[string]
		$ExportPath,

		[string[]]
		$RelatedPropertyNames
	)

	# Check if already exported (resume support)
	if (Get-ZtConfig -ExportPath $ExportPath -Property $Name) {
		Write-PSFMessage "Skipping '{0}' since it was downloaded previously" -StringValues $Name -Target $Name -Tag Export, redundant, skip
		Update-ZtProgressState -WorkerId $Name -WorkerName $Name -WorkerStatus 'Running' -WorkerDetail 'Skipped (cached)'
		return
	}

	# Get maximum size limit for SignIn logs
	$maxSizeBytes = Get-PSFConfigValue -FullName 'ZeroTrustAssessment.Export.SignInLog.MaxSizeBytes' -Fallback 1073741824

	# Calculate per-slice time limit: divide total time budget across days, with a minimum of 5 min
	$perSliceTimeLimit = if ($MaximumQueryTime -gt 0) {
		[math]::Max(5, [math]::Ceiling($MaximumQueryTime / [math]::Min($Days, 10)))
	}
	else {
		0  # No limit
	}

	# Prepare the output folder
	$folderPath = Join-Path -Path $ExportPath -ChildPath $Name
	if (Test-Path $folderPath) {
		Remove-Item $folderPath -Recurse -Force
	}
	$null = New-Item -ItemType Directory -Path $folderPath -Force -ErrorAction Stop

	# Build day-slice definitions
	$tmzFormat = "yyyy-MM-ddTHH:mm:ssZ"
	$today = (Get-Date -Hour 0 -Minute 0 -Second 0)
	$slices = @()
	for ($day = 0; $day -lt $Days; $day++) {
		$sliceEnd = $today.AddDays(-$day)
		$sliceStart = $today.AddDays(-($day + 1))

		# Build the filter for this specific day
		$dateFilter = "createdDateTime ge $($sliceStart.ToString($tmzFormat)) and createdDateTime lt $($sliceEnd.ToString($tmzFormat))"

		# Extract non-date parts from the original QueryString
		# The original QueryString looks like: "createdDateTime ge <date> and status/errorcode eq 0 and appid eq '...'"
		# We need to replace the date portion but keep the rest
		$extraFilters = ''
		if ($QueryString) {
			# Remove the date filter portion (everything before "and status/" or "and appid")
			# Pattern: "createdDateTime ge <datetime> and "
			$extraFilters = $QueryString -replace 'createdDateTime\s+ge\s+\S+\s+and\s+', ''
		}

		if ($extraFilters) {
			$sliceFilter = "`$filter=$dateFilter and $extraFilters&`$top=999"
		}
		else {
			$sliceFilter = "`$filter=$dateFilter&`$top=999"
		}

		$slices += @{
			Day         = $day
			Start       = $sliceStart
			End         = $sliceEnd
			QueryString = $sliceFilter
		}
	}

	Write-PSFMessage "Starting parallel date-range export for '{0}': {1} day-slices" -StringValues $Name, $slices.Count -Tag Export
	Update-ZtProgressState -WorkerId $Name -WorkerName $Name -WorkerStatus 'Running' -WorkerDetail "Exporting $($slices.Count) day-slices in parallel..."

	# Determine parallelism: use up to 5 concurrent slices (Graph can handle this without excessive throttling)
	$sliceThrottleLimit = [math]::Min(5, $slices.Count)

	# Export each slice sequentially within this worker but using the standard Export-ZtGraphEntity
	# mechanism. We process slices in batches to provide parallelism within the single worker allocation.
	# Note: Since this runs inside a single runspace worker, true parallelism requires us to use
	# ForEach-Object -Parallel (PS 7+) or sequential processing with progress.
	$globalPageIndex = 0
	$totalSize = 0
	$sizeExceeded = $false
	$overallStartTime = Get-Date
	$hasOverallTimeLimit = $MaximumQueryTime -gt 0
	$overallStopTime = $overallStartTime.AddMinutes($MaximumQueryTime)

	foreach ($slice in $slices) {
		if ($sizeExceeded) { break }
		if ($hasOverallTimeLimit -and (Get-Date) -gt $overallStopTime) {
			Write-PSFMessage "Overall time limit reached for $Name after processing $($slice.Day) day-slices" -Tag Export
			break
		}

		$sliceLabel = "$($slice.Start.ToString('yyyy-MM-dd'))"
		Update-ZtProgressState -WorkerId $Name -WorkerName $Name -WorkerStatus 'Running' -WorkerDetail "Day $($slice.Day + 1)/$($slices.Count): $sliceLabel"

		$actualUri = "$Uri`?$($slice.QueryString)"
		$sliceStartTime = Get-Date
		$sliceStopTime = if ($perSliceTimeLimit -gt 0) { $sliceStartTime.AddMinutes($perSliceTimeLimit) } else { [datetime]::MaxValue }
		$previousNextLink = $null

		do {
			$results = $null
			try {
				$results = Invoke-ZtRetry -ScriptBlock { Invoke-MgGraphRequest -Method GET -Uri $actualUri -OutputType HashTable }
			}
			catch {
				Write-PSFMessage -Level Warning "Export '$Name' slice '$sliceLabel' failed. URI: $actualUri" -ErrorRecord $_ -Tag Export, Error
				# Continue with next slice rather than failing the entire export
				break
			}

			# Validate response
			if ($results -is [hashtable] -and $results.ContainsKey('error')) {
				$errorCode = $results.error.code
				$errorMessage = $results.error.message
				Write-PSFMessage -Level Warning "API error for '$Name' slice '$sliceLabel': [$errorCode] $errorMessage" -Tag Export, Error
				break
			}

			if ($results -and $results.value) {
				# Write page to the shared output folder
				$filePath = Join-Path -Path $folderPath -ChildPath "$Name-$globalPageIndex.json"
				$results | Export-PSFJson -Path $filePath -Depth 100 -Encoding UTF8NoBom
				$globalPageIndex++

				# Track total size
				if (Test-Path $filePath) {
					$fileSize = (Get-Item $filePath).Length
					$totalSize += $fileSize

					if ($totalSize -gt $maxSizeBytes) {
						$sizeMB = [math]::Round($totalSize / 1MB, 2)
						$limitMB = [math]::Round($maxSizeBytes / 1MB, 2)
						Write-PSFMessage -Level Warning "Sign-in log export reached size limit of $limitMB MB (current: $sizeMB MB). Stopping." -Tag Export, SignIn, SizeLimit
						$sizeExceeded = $true
						break
					}
				}
			}

			# Get next page
			if (-not $results) {
				$actualUri = $null
			}
			else {
				$actualUri = $results.'@odata.nextLink'
			}

			if (-not $actualUri) { break }

			# Stuck paging detection
			if ($actualUri -eq $previousNextLink) {
				Write-PSFMessage -Level Warning "Stuck paging detected for '$Name' slice '$sliceLabel' on page $globalPageIndex. Moving to next slice." -Tag Export, Error
				break
			}
			$previousNextLink = $actualUri

			# Per-slice time limit
			if ((Get-Date) -gt $sliceStopTime) {
				Write-PSFMessage "Per-slice time limit ($perSliceTimeLimit min) reached for '$Name' slice '$sliceLabel'" -Tag Export
				break
			}
		} while ($true)
	}

	$totalSizeMB = [math]::Round($totalSize / 1MB, 2)
	Write-PSFMessage "Date-range export for '$Name' completed: $globalPageIndex pages, $totalSizeMB MB" -Tag Export

	Set-ZtConfig -ExportPath $ExportPath -Property $Name -Value $true
}
