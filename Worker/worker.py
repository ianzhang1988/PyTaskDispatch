# -*- coding: utf-8 -*-
# @Time    : 2020/1/7 10:04
# @Author  : ZhangYang
# @Email   : ian.zhang.88@outlook.com

import asyncio
import re
import time
import os

class SceneCut():
    def __init__(self):
        pass

    async def get_scene_cut(self, input_file):

        proc = await asyncio.create_subprocess_shell(
            "ffmpeg -i %s -filter:v select='gt(scene,0.3)',showinfo -f null -" % input_file,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)

        # proc = await asyncio.create_subprocess_exec('ffmpeg', '-i', 'star.war.4.270s.mp4', '-filter:v', "select='gt(scene,0.3)',showinfo", '-f', 'null', '-',
        #                                              stdout=asyncio.subprocess.PIPE,
        #                                              stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await proc.communicate()

        s = time.perf_counter()
        # test_str = '[Parsed_showinfo_1 @ 0000000003fc0520] n:   4 pts: 465808 pts_time:29.113  pos:  6431648 fmt:rgb24 sar:1/1 s:1024x576 i:P iskey:0 type:P checksum:1EE4A549 plane_checksum:[1EE4A549] mean:[185] stdev:[105.0]'
        found = re.findall(r'\[\w*?showinfo.*?\].*?pts_time:(\d*\.?\d*?) .*? iskey:(\d) .*?', stderr.decode('utf-8'))
        # print(found)

class MediaInfo():

    class ParseError(Exception): pass

    def __init__(self, input_file):
        self.input_file = input_file
        self.video_duration = -1
        self.audio_duration = -1

    async def parse(self):
        proc = await asyncio.create_subprocess_shell(
            "mediainfo -f %s" % self.input_file,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)

        stdout, _ = await proc.communicate()

        content=stdout.decode('utf-8')

        # print(stdout.decode('utf-8'))

        sections = content.split('\r\n\r\n')

        for s in sections:
            lines = s.split('\r\n')
            if lines[0] == 'Video':
                self._parse_video(s)
            elif lines[0] == 'Audio':
                self._parse_audio(s)


    def _parse_video(self, content):
        found = re.findall(r'Duration +: (\d+)\r\n', content)
        if not found:
            raise MediaInfo.ParseError('not found duration')
        duration = int(found[0])
        if duration < self.video_duration:
            return

        self.video_duration = duration

        # other attribute

    def _parse_audio(self, content):
        found = re.findall(r'Duration +: (\d+)\r\n', content)
        if not found:
            raise MediaInfo.ParseError('not found duration')
        duration = int(found[0])
        if duration < self.audio_duration:
            return

        self.audio_duration = duration

        # other attribute


async def main():
    input = 'star.war.4.270s.mp4'
    base_output_path = 'output'
    # sc = SceneCut()
    # scene_cut_list = await sc.get_scene_cut(input)
    # print(scene_cut_list)

    mi = MediaInfo(input)
    await mi.parse()
    print(mi.video_duration)
    print(mi.audio_duration)


    split_points = []

    split_interval_ms = 5000
    next_point = split_interval_ms
    end_point = mi.video_duration

    while True:
        if next_point > end_point:
            break

        split_points.append(next_point)

        next_point += split_interval_ms

    # merge last two piece
    split_points.pop()

    print(split_points)
    print(end_point - split_points[-1])

    start_point_list = []
    start_point_list.extend(split_points)
    start_point_list.insert(0, 0)
    end_point_list = []
    end_point_list.extend(split_points)
    end_point_list.append(end_point)

    start_end_time = [(float(i[0])/1000, float(i[1])/1000) for i in zip(start_point_list, end_point_list)]


    ffmpeg_cmd = 'ffmpeg -ss {start} -t {duration} -i {input} -crf {crf} -c:v libx264 -vf scale=-2:320 -c:a aac -b:a 96k -ac 2 -f mp4 {output}'

    crf_range = list(range(32, 22, -2))

    ffmpeg_cmd_list = []

    for idx,(start, end) in enumerate(start_end_time):
        duration = end - start
        for crf in crf_range:
            output = f'out_{crf}_{idx:05d}.mp4'
            output = os.path.join(base_output_path, output)
            cmd = ffmpeg_cmd.format(start=start, duration=duration, input=input, crf=crf, output=output)

            ffmpeg_cmd_list.append(cmd)

    for i in ffmpeg_cmd_list:
        print(i)

    print(len(ffmpeg_cmd_list))

    vmaf_cmd = 'ffmpeg '



if __name__ == '__main__':
    asyncio.run(main())