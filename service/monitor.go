// Created by LiuSainan on 2023-03-04 19:08:39

package service

import (
	"strconv"
	"strings"
)

type RedisMonitorLine struct {
	Timestamp string
	DB        string
	Client    string
	Cmd       string
	Args      []interface{}
}

func NewRedisMonitorLine(line []string) (*RedisMonitorLine, error) {
	var err error
	out := &RedisMonitorLine{
		Timestamp: line[0],
		DB:        strings.TrimLeft(line[1], "["),
		Client:    strings.TrimRight(line[2], "]"),
	}

	out.Cmd, err = strconv.Unquote(line[3])
	if err != nil {
		return out, err
	}

	for i := 4; i < len(line); i++ {
		arg, err := strconv.Unquote(line[i])
		if err != nil {
			return out, err
		}
		out.Args = append(out.Args, arg)
	}
	return out, nil
}
