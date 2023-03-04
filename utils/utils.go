// Created by LiuSainan on 2023-03-03 23:46:22

package utils

import "strings"

func RedisMonitorLineSplit(line string) ([]string, error) {
	var tmp strings.Builder
	var lineSlices []string
	var status int
	for i := 0; i < len(line); i++ {
		if string(line[i]) == "\"" && (i == 0 || string(line[i-1]) != "\\") {
			status += 1
		}

		if string(line[i]) == " " && status != 1 {
			lineSlices = append(lineSlices, tmp.String())
			status = 0
			tmp.Reset()
			continue
		}

		if _, err := tmp.WriteString(string(line[i])); err != nil {
			return nil, err
		}
	}

	if status == 2 {
		lineSlices = append(lineSlices, tmp.String())
	}

	return lineSlices, nil
}
