package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
)

func main() {
	// CLI args
	f, closeFile, err := openProcessingFile(os.Args...)
	if err != nil {
		log.Fatal(err)
	}
	defer closeFile()

	// Load and parse processes
	processes, err := loadProcesses(f)
	if err != nil {
		log.Fatal(err)
	}

	// First-come, first-serve scheduling
	FCFSSchedule(os.Stdout, "First-come, first-serve", processes)
	
	// Shortest-job-first
	SJFSchedule(os.Stdout, "Shortest-job-first", processes)
	
	// Shortest-job-first Priority
	SJFPrioritySchedule(os.Stdout, "Priority", processes)
	
	// Round-robin Scheduling
	RRSchedule(os.Stdout, "Round-robin", processes)
}

func openProcessingFile(args ...string) (*os.File, func(), error) {
	if len(args) != 2 {
		return nil, nil, fmt.Errorf("%w: must give a scheduling file to process", ErrInvalidArgs)
	}
	// Read in CSV process CSV file
	f, err := os.Open(args[1])
	if err != nil {
		return nil, nil, fmt.Errorf("%v: error opening scheduling file", err)
	}
	closeFn := func() {
		if err := f.Close(); err != nil {
			log.Fatalf("%v: error closing scheduling file", err)
		}
	}

	return f, closeFn, nil
}

type (
	Process struct {
		ProcessID     int64
		ArrivalTime   int64
		BurstDuration int64
		Priority      int64
	}
	TimeSlice struct {
		PID   int64
		Start int64
		Stop  int64
	}
)

//region Schedulers

// FCFSSchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
// • an output writer
// • a title for the chart
// • a slice of processes
func FCFSSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)
	for i := range processes {
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime
		}
		totalWait += float64(waitingTime)

		start := waitingTime + processes[i].ArrivalTime

		turnaround := processes[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += processes[i].BurstDuration

		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

// Shortest-job-first Scheduling
func SJFSchedule(w io.Writer, title string, processes []Process) {
	var (
		totalWait        float64
		totalTurnaround  float64
		lastCompletion   float64
		schedule         = make([][]string, len(processes))
		gantt            = make([]TimeSlice, 0)
		RemainingBurst	 = make([]int, len(processes))
	)
	
	lastGanttIndex := -1
	lastGanttStartTime := 0
	totalBurstTime := 0

	for i := range processes {
		totalBurstTime += int(processes[i].BurstDuration)
		RemainingBurst[i] = int(processes[i].BurstDuration)
	}

	for tick := 0; tick < totalBurstTime; {
		ShortestJobIndex := -1
		ShortestJobBurst := 100000

		// Find shortest process that exists
		for i := range processes {
			if RemainingBurst[i] > 0 && processes[i].ArrivalTime <= int64(tick) && RemainingBurst[i] <= ShortestJobBurst {
				ShortestJobIndex = i
				ShortestJobBurst = RemainingBurst[i]
			}
		}
		
		// Take care of gantt table
		if lastGanttIndex != ShortestJobIndex || tick == totalBurstTime - 1 {
			if tick == totalBurstTime - 1 {
				gantt = append(gantt, TimeSlice{
					PID:   processes[lastGanttIndex].ProcessID,
					Start: int64(lastGanttStartTime),
					Stop:  int64(tick+1),
				})
			} else if lastGanttIndex != -1 {
				gantt = append(gantt, TimeSlice{
					PID:   processes[lastGanttIndex].ProcessID,
					Start: int64(lastGanttStartTime),
					Stop:  int64(tick),
				})
			}
			lastGanttStartTime = tick
			lastGanttIndex = ShortestJobIndex
		}
		
		//Increase tick
		tick++

		if ShortestJobIndex == -1 {
			totalBurstTime++
		} else {
		
			RemainingBurst[ShortestJobIndex]--
		
			//If process done, then schedule
			if RemainingBurst[ShortestJobIndex] == 0 {
				totalTurnaround += float64(int64(tick) - processes[ShortestJobIndex].ArrivalTime)
				waitTime := float64(tick - int(processes[ShortestJobIndex].ArrivalTime) - int(processes[ShortestJobIndex].BurstDuration))
				totalWait += waitTime

				schedule[ShortestJobIndex] = []string{
					fmt.Sprint(processes[ShortestJobIndex].ProcessID),
					fmt.Sprint(processes[ShortestJobIndex].Priority),
					fmt.Sprint(processes[ShortestJobIndex].BurstDuration),
					fmt.Sprint(processes[ShortestJobIndex].ArrivalTime),
					fmt.Sprint(tick - int(processes[ShortestJobIndex].ArrivalTime) - int(processes[ShortestJobIndex].BurstDuration)),
					fmt.Sprint(tick - int(processes[ShortestJobIndex].ArrivalTime)),
					fmt.Sprint(tick),
				}

				lastCompletion = float64(processes[ShortestJobIndex].BurstDuration + processes[ShortestJobIndex].ArrivalTime + int64(waitTime))
			}
		}
	}
	//Calculate Averages
	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion
	//Output Averages
	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

// Shortest-job-first Priority Scheduling
func SJFPrioritySchedule(w io.Writer, title string, processes []Process) {
	var (
		totalWait        float64
		totalTurnaround  float64
		lastCompletion   float64
		schedule         = make([][]string, len(processes))
		gantt            = make([]TimeSlice, 0)
		RemainingBurst	 = make([]int, len(processes))
	)
	
	lastGanttIndex := -1
	lastGanttStartTime := 0
	totalBurstTime := 0

	for i := range processes {
		totalBurstTime += int(processes[i].BurstDuration)
		RemainingBurst[i] = int(processes[i].BurstDuration)
	}

	for tick := 0; tick < totalBurstTime; {
		ShortestJobPriority := 100000
		ShortestJobIndex    := -1
		ShortestJobBurst 	:= 100000

		// Find shortest process that exists
		for i := range processes {
			if RemainingBurst[i] > 0 && processes[i].ArrivalTime <= int64(tick) {
				//Priority Check
				if processes[i].Priority <= int64(ShortestJobPriority) && processes[i].Priority < int64(ShortestJobPriority) {
					ShortestJobIndex = i
					ShortestJobBurst = int(processes[i].BurstDuration)
					ShortestJobPriority = int(processes[i].Priority)
				} else if RemainingBurst[i] < ShortestJobBurst {
					ShortestJobIndex = i
					ShortestJobBurst = RemainingBurst[i]
					ShortestJobPriority = int(processes[i].Priority)
				}
			}
		}
		// Take care of gantt table
		if lastGanttIndex != ShortestJobIndex || tick == totalBurstTime - 1 {
			if tick == totalBurstTime - 1 {
				gantt = append(gantt, TimeSlice{
					PID:   processes[lastGanttIndex].ProcessID,
					Start: int64(lastGanttStartTime),
					Stop:  int64(tick+1),
				})
			} else if lastGanttIndex != -1 {
				gantt = append(gantt, TimeSlice{
					PID:   processes[lastGanttIndex].ProcessID,
					Start: int64(lastGanttStartTime),
					Stop:  int64(tick),
				})
			}
			lastGanttStartTime = tick
			lastGanttIndex = ShortestJobIndex
		}
		
		//Increase tick
		tick++

		if ShortestJobIndex == -1 {
			totalBurstTime++
		} else {
		
			RemainingBurst[ShortestJobIndex]--
		
			//If process done, then schedule
			if RemainingBurst[ShortestJobIndex] == 0 {
				totalTurnaround += float64(int64(tick) - processes[ShortestJobIndex].ArrivalTime)
				waitTime := float64(tick - int(processes[ShortestJobIndex].ArrivalTime) - int(processes[ShortestJobIndex].BurstDuration))
				totalWait += waitTime

				schedule[ShortestJobIndex] = []string{
					fmt.Sprint(processes[ShortestJobIndex].ProcessID),
					fmt.Sprint(processes[ShortestJobIndex].Priority),
					fmt.Sprint(processes[ShortestJobIndex].BurstDuration),
					fmt.Sprint(processes[ShortestJobIndex].ArrivalTime),
					fmt.Sprint(tick - int(processes[ShortestJobIndex].ArrivalTime) - int(processes[ShortestJobIndex].BurstDuration)),
					fmt.Sprint(tick - int(processes[ShortestJobIndex].ArrivalTime)),
					fmt.Sprint(tick),
				}
				lastCompletion = float64(processes[ShortestJobIndex].BurstDuration + processes[ShortestJobIndex].ArrivalTime + int64(waitTime))
			}
		}
	}
	//Calculate Averages
	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion
	//Output Averages
	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}
	

//Round-Robin Scheduling
func RRSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
		QuantumTime		int64
		OGBurst			= make([]int64, len(processes))
	)
	
	//OGBurst := [0]int{0}
	tick := 0
	QuantumTime = 3
	
	
	for tick < 1000 { 
	
		for i := range processes {
			if processes[i].BurstDuration >= QuantumTime {

			waitingTime = serviceTime - processes[i].ArrivalTime
			
			if OGBurst[i] == 0 { 
				OGBurst[i] = processes[i].BurstDuration
			}
			
			start := waitingTime + processes[i].ArrivalTime
	
			turnaround := QuantumTime + waitingTime

			completion := QuantumTime + processes[i].ArrivalTime + waitingTime
			lastCompletion = float64(completion)

			waitingTime = (turnaround - OGBurst[i])
			
			schedule[i] = []string{
				fmt.Sprint(processes[i].ProcessID),
				fmt.Sprint(processes[i].Priority),
				fmt.Sprint(OGBurst[i]),
				fmt.Sprint(processes[i].ArrivalTime),
				fmt.Sprint(waitingTime),
				fmt.Sprint(turnaround),
				fmt.Sprint(completion),
			}
			serviceTime += QuantumTime

			gantt = append(gantt, TimeSlice{
				PID:   processes[i].ProcessID,
				Start: start,
				Stop:  serviceTime,
			})
			
			processes[i].BurstDuration -= QuantumTime
			
			if processes[i].BurstDuration == 0 {
				totalWait += float64(waitingTime)
				totalTurnaround += float64(turnaround)
			}
			
			
			} else if processes[i].BurstDuration < QuantumTime && processes[i].BurstDuration > 0 {

			waitingTime = serviceTime - processes[i].ArrivalTime
			
			if OGBurst[i] == 0 { 
				OGBurst[i] = processes[i].BurstDuration
			}
			
			start := waitingTime + processes[i].ArrivalTime
	
			turnaround := processes[i].BurstDuration + waitingTime
			//totalTurnaround += float64(turnaround)

			completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
			lastCompletion = float64(completion)
			
			waitingTime = (turnaround - OGBurst[i])
			
			schedule[i] = []string{
				fmt.Sprint(processes[i].ProcessID),
				fmt.Sprint(processes[i].Priority),
				fmt.Sprint(OGBurst[i]),
				fmt.Sprint(processes[i].ArrivalTime),
				fmt.Sprint(waitingTime),
				fmt.Sprint(turnaround),
				fmt.Sprint(completion),
			}
			serviceTime += processes[i].BurstDuration

			gantt = append(gantt, TimeSlice{
				PID:   processes[i].ProcessID,
				Start: start,
				Stop:  serviceTime,
			})
			
			processes[i].BurstDuration -= processes[i].BurstDuration
			
			if processes[i].BurstDuration == 0 {
				totalWait += float64(waitingTime)
				totalTurnaround += float64(turnaround)
			}
			
			} 
		}
		//Increase tick
		tick++
	}
	//Calculate Averages
	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion
	//Output Averages
	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
	
}
	

//endregion

//region Output helpers

func outputTitle(w io.Writer, title string) {
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
	_, _ = fmt.Fprintln(w, strings.Repeat(" ", len(title)/2), title)
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
}

func outputGantt(w io.Writer, gantt []TimeSlice) {
	_, _ = fmt.Fprintln(w, "Gantt schedule")
	_, _ = fmt.Fprint(w, "|")
	for i := range gantt {
		pid := fmt.Sprint(gantt[i].PID)
		padding := strings.Repeat(" ", (8-len(pid))/2)
		_, _ = fmt.Fprint(w, padding, pid, padding, "|")
	}
	_, _ = fmt.Fprintln(w)
	for i := range gantt {
		_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Start), "\t")
		if len(gantt)-1 == i {
			_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Stop))
		}
	}
	_, _ = fmt.Fprintf(w, "\n\n")
}

func outputSchedule(w io.Writer, rows [][]string, wait, turnaround, throughput float64) {
	_, _ = fmt.Fprintln(w, "Schedule table")
	table := tablewriter.NewWriter(w)
	table.SetHeader([]string{"ID", "Priority", "Burst", "Arrival", "Wait", "Turnaround", "Exit"})
	table.AppendBulk(rows)
	table.SetFooter([]string{"", "", "", "",
		fmt.Sprintf("Average\n%.2f", wait),
		fmt.Sprintf("Average\n%.2f", turnaround),
		fmt.Sprintf("Throughput\n%.2f/t", throughput)})
	table.Render()
}

//endregion

//region Loading processes.

var ErrInvalidArgs = errors.New("invalid args")

func loadProcesses(r io.Reader) ([]Process, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%w: reading CSV", err)
	}

	processes := make([]Process, len(rows))
	for i := range rows {
		processes[i].ProcessID = mustStrToInt(rows[i][0])
		processes[i].BurstDuration = mustStrToInt(rows[i][1])
		processes[i].ArrivalTime = mustStrToInt(rows[i][2])
		if len(rows[i]) == 4 {
			processes[i].Priority = mustStrToInt(rows[i][3])
		}
	}

	return processes, nil
}

func mustStrToInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	return i
}

//endregion
