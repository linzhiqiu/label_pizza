import streamlit.components.v1 as components
import re
from urllib.parse import urlparse, parse_qs

###############################################################################
# YOUTUBE DETECTION AND UTILITIES
###############################################################################

def is_youtube_url(url):
    """Check if URL is a YouTube video using proper URL parsing"""
    try:
        parsed = urlparse(url.lower())
        youtube_domains = ['youtube.com', 'www.youtube.com', 'm.youtube.com', 'youtu.be']
        return parsed.netloc in youtube_domains
    except:
        return False

def extract_youtube_id(url):
    """Extract video ID from YouTube URL (including embed URLs)"""
    patterns = [
        r'(?:v=|/)([0-9A-Za-z_-]{11}).*',
        r'(?:embed/)([0-9A-Za-z_-]{11})',
        r'(?:youtu\.be/)([0-9A-Za-z_-]{11})'
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None

def extract_youtube_params(url):
    """Extract start and end parameters from YouTube URL"""
    try:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        
        start = params.get('start', [None])[0] or params.get('t', [None])[0]
        end = params.get('end', [None])[0]
        
        # Handle 't' parameter which can be like '1m30s' or just seconds
        if start and 'm' in str(start):
            match = re.match(r'(?:(\d+)m)?(?:(\d+)s?)?', str(start))
            if match:
                minutes = int(match.group(1) or 0)
                seconds = int(match.group(2) or 0)
                start = minutes * 60 + seconds
        
        # Convert to integers if they exist
        if start:
            start = int(start)
        if end:
            end = int(end)
            
        return start, end
    except:
        return None, None

def get_download_filename(video_uid, title=None):
    """Get appropriate download filename from video_uid or title"""
    if title:
        # Clean title for filename
        clean_title = re.sub(r'[<>:"/\\|?*]', '', title)
        clean_title = clean_title.strip()
        if clean_title:
            return f"{clean_title}.mp4"
    
    # Use video_uid if it already has a proper extension
    if '.' in video_uid and len(video_uid.split('.')[-1]) <= 4:
        return video_uid
    else:
        return f"{video_uid}.mp4"

def get_youtube_title(video_url):
    """Get YouTube video title using oEmbed API"""
    try:
        video_id = extract_youtube_id(video_url)
        if not video_id:
            return None
            
        import requests
        oembed_url = f"https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v={video_id}&format=json"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        
        response = requests.get(oembed_url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            return data.get('title', 'YouTube Video')
    except:
        pass
    
    return None

###############################################################################
# MAIN VIDEO PLAYER FUNCTION
###############################################################################

def custom_video_player(video_url, video_uid=None, aspect_ratio="16:9", autoplay=True, loop=True, show_share_button=False):
    """Universal video player supporting both direct files and YouTube with custom controls"""
    
    # Initialize variables
    video_title = None
    start_time = None
    end_time = None
    is_youtube = False
    
    # Fallback video_uid extraction if not provided
    if video_uid is None:
        video_uid = video_url.split('/')[-1].split('?')[0]
    
    # Check if this is a YouTube video
    if is_youtube_url(video_url):
        is_youtube = True
        youtube_id = extract_youtube_id(video_url)
        
        if not youtube_id:
            # Invalid YouTube URL - show error
            error_html = """
            <div style="display: flex; align-items: center; justify-content: center; height: 300px; background: #f8f9fa; border: 2px dashed #dee2e6; border-radius: 8px;">
                <div style="text-align: center; color: #6c757d;">
                    <h3>❌ Invalid YouTube URL</h3>
                    <p>Could not extract video ID from the provided YouTube URL</p>
                </div>
            </div>
            """
            components.html(error_html, height=300, scrolling=False)
            return 300
        
        # Extract YouTube parameters for start/end time (only for YouTube)
        start_time, end_time = extract_youtube_params(video_url)
        
        # Get video title
        video_title = get_youtube_title(video_url)
        if video_title and video_uid == youtube_id:  # Only update if using default
            video_uid = video_title
    
    # Use custom player for both direct URLs and YouTube
    return _render_custom_player(
        video_url, 
        video_uid, 
        aspect_ratio, 
        autoplay, 
        loop, 
        show_share_button, 
        video_title,
        start_time,
        end_time,
        is_youtube
    )

###############################################################################
# CUSTOM PLAYER RENDERER
###############################################################################

def _render_custom_player(video_url, video_uid, aspect_ratio, autoplay, loop, show_share_button, video_title=None, start_time=None, end_time=None, is_youtube=False):
    """Render custom video player with all controls"""
    
    ratio_parts = aspect_ratio.split(":")
    aspect_ratio_decimal = float(ratio_parts[0]) / float(ratio_parts[1])
    padding_bottom = (1 / aspect_ratio_decimal) * 100
    
    # Get proper download filename
    download_filename = get_download_filename(video_uid, video_title)

    # Share button
    share_button_html = ""
    share_button_js = ""

    if show_share_button:
        share_button_html = '''<button class="control-btn" id="shareBtn" title="Copy search link">🔗</button>'''
        
        share_button_js = f'''
            const shareBtn = document.getElementById('shareBtn');
            if (shareBtn) {{
                shareBtn.addEventListener('click', () => {{
                    const currentUrl = window.parent.location.href.split('?')[0];
                    const searchPortalUrl = currentUrl + '?video_uid={video_uid}';
                    navigator.clipboard.writeText(searchPortalUrl).then(() => {{
                        shareBtn.innerHTML = '✓';
                        setTimeout(() => shareBtn.innerHTML = '🔗', 2000);
                    }}).catch(e => console.log('Copy failed:', e));
                }});
            }}
        '''
    
    # Video source info (for URL generation only)
    youtube_source_url = ""
    if is_youtube:
        youtube_id = extract_youtube_id(video_url)
        youtube_source_url = f"https://www.youtube.com/watch?v={youtube_id}"
        if start_time is not None or end_time is not None:
            params = []
            if start_time is not None:
                params.append(f"t={start_time}")
            if end_time is not None:
                params.append(f"end={end_time}")
            if params:
                youtube_source_url += "&" + "&".join(params)
    
    # Video player HTML - different for YouTube vs direct
    video_player_html = ""
    if is_youtube:
        youtube_id = extract_youtube_id(video_url)
        video_player_html = f'''
            <div id="youtubeContainer" style="position: absolute; top: 0; left: 0; width: 100%; height: 100%; overflow: hidden;">
                <div id="youtubePlayer" style="width: 100%; height: 100%;"></div>
            </div>
        '''
    else:
        video_attributes = 'preload="metadata" muted'
        if autoplay:
            video_attributes += ' autoplay muted'
        if loop:  # Natural looping for direct videos
            video_attributes += ' loop'
        
        video_player_html = f'''
            <video id="customVideo" {video_attributes} style="position: absolute; top: 0; left: 0; width: 100%; height: 100%; object-fit: contain;">
                <source src="{video_url}" type="video/mp4">
                Your browser does not support the video tag.
            </video>
        '''
    
    # Segment play button (only show for YouTube with time constraints)
    segment_button_html = ""
    if is_youtube and (start_time is not None or end_time is not None):
        segment_button_html = '<button class="control-btn active" id="segmentBtn" title="Stop Segment Loop">⏹️</button>'
    
    # Download/Source button
    download_btn_action = ""
    if is_youtube:
        download_btn_action = f'''
            downloadBtn.addEventListener('click', () => {{
                window.open('{youtube_source_url}', '_blank');
            }});
        '''
    else:
        download_btn_action = f'''
            downloadBtn.addEventListener('click', async () => {{
                try {{
                    const response = await fetch('{video_url}');
                    const blob = await response.blob();
                    const url = window.URL.createObjectURL(blob);
                    const a = document.createElement('a');
                    a.href = url;
                    a.download = '{download_filename}';
                    a.style.display = 'none';
                    document.body.appendChild(a);
                    a.click();
                    document.body.removeChild(a);
                    window.URL.revokeObjectURL(url);
                }} catch (error) {{
                    console.error('Download failed:', error);
                    const a = document.createElement('a');
                    a.href = '{video_url}';
                    a.download = '{download_filename}';
                    a.target = '_blank';
                    a.click();
                }}
            }});
        '''

    html_code = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <script src="https://www.youtube.com/iframe_api"></script>
        <style>
            * {{ box-sizing: border-box; margin: 0; padding: 0; }}
            html, body {{ height: 100%; font-family: Arial, sans-serif; overflow: hidden; }}
            
            .video-container {{
                width: 100%; height: 100%; display: flex; flex-direction: column;
                background: #fff; overflow: hidden;
            }}
            
            .video-wrapper {{
                position: relative; width: 100%; flex: 1; background: #000;
                border-radius: 8px 8px 0 0; overflow: hidden; min-height: 200px;
            }}
            
            .video-wrapper::before {{
                content: ''; display: block; padding-bottom: {padding_bottom}%;
            }}
            
            video {{
                position: absolute; top: 0; left: 0; width: 100%; height: 100%;
                object-fit: contain;
            }}
            
            video::-webkit-media-controls, video::-moz-media-controls {{
                display: none !important;
            }}
            
            .controls-container {{
                width: 100%; background: #f8f9fa; border: 1px solid #e9ecef;
                border-top: none; border-radius: 0 0 8px 8px; padding: 8px 12px;
                flex-shrink: 0; overflow: hidden; min-height: 65px; max-height: 80px;
            }}
            
            .progress-container {{
                width: 100%; height: 6px; background: #ddd; border-radius: 3px;
                margin-bottom: 8px; cursor: pointer; position: relative;
                user-select: none; overflow: hidden;
            }}
            
            .progress-bar {{
                height: 100%; background: #9553FE;
                border-radius: 3px; width: 0%; pointer-events: none; transition: none;
            }}
            
            .time-range {{
                position: absolute; top: 0; height: 100%; background: rgba(149, 83, 254, 0.3);
                border-radius: 3px; pointer-events: none;
            }}
            
            .progress-handle {{
                position: absolute; top: -5px; width: 16px; height: 16px;
                background: #9553FE; border: 2px solid white; border-radius: 50%;
                cursor: grab; transform: translateX(-50%); opacity: 0;
                transition: opacity 0.2s ease, transform 0.1s ease;
                box-shadow: 0 2px 4px rgba(0,0,0,0.2);
            }}
            
            .progress-handle:active {{ cursor: grabbing; transform: translateX(-50%) scale(1.1); }}
            .progress-container:hover .progress-handle {{ opacity: 1; }}
            
            .controls {{
                display: flex; align-items: center; gap: 6px; width: 100%;
                overflow: hidden; min-height: 32px;
            }}
            
            .control-btn {{
                background: none; border: none; font-size: 14px; cursor: pointer;
                padding: 4px 6px; border-radius: 4px; transition: background 0.2s ease;
                display: flex; align-items: center; justify-content: center;
                min-width: 28px; height: 28px; flex-shrink: 0;
            }}
            
            .control-btn:hover {{ background: #e9ecef; }}
            .control-btn:disabled {{ opacity: 0.5; cursor: not-allowed; }}
            .control-btn.active {{ background: #9553FE; color: white; }}
            .control-btn.active:hover {{ background: #7c3aed; }}
            
            .time-display {{
                font-size: 11px; color: #666; margin-left: auto; white-space: nowrap;
                font-family: 'Courier New', monospace; flex-shrink: 0;
                overflow: hidden; text-overflow: ellipsis; max-width: 120px;
            }}
            
            .volume-control {{ display: flex; align-items: center; gap: 4px; flex-shrink: 0; }}
            
            .volume-slider {{
                width: 50px; height: 3px; background: #ddd; outline: none;
                border-radius: 2px; -webkit-appearance: none; flex-shrink: 0;
            }}
            
            .volume-slider::-webkit-slider-thumb {{
                -webkit-appearance: none; width: 12px; height: 12px;
                background: #9553FE; border-radius: 50%; cursor: pointer;
            }}
            
            .volume-slider::-moz-range-thumb {{
                width: 12px; height: 12px; background: #9553FE;
                border-radius: 50%; cursor: pointer; border: none;
            }}
            
            @media (max-width: 600px) {{
                .controls {{ gap: 4px; }}
                .control-btn {{ font-size: 12px; min-width: 24px; height: 24px; padding: 2px 4px; }}
                .time-display {{ font-size: 10px; max-width: 80px; }}
                .volume-slider {{ width: 40px; }}
                .controls-container {{ padding: 6px 8px; min-height: 60px; }}
                .progress-container {{ height: 5px; margin-bottom: 6px; }}
            }}
        </style>
    </head>
    <body>
        <div class="video-container">
            <div class="video-wrapper">
                {video_player_html}
            </div>
            
            <div class="controls-container">
                <div class="progress-container" id="progressContainer">
                    <div class="time-range" id="timeRange"></div>
                    <div class="progress-bar" id="progressBar"></div>
                    <div class="progress-handle" id="progressHandle"></div>
                </div>
                
                <div class="controls">
                    <button class="control-btn" id="playPauseBtn" title="Play/Pause">{"⏸️" if autoplay else "▶️"}</button>
                    <button class="control-btn" id="muteBtn" title="Mute/Unmute">🔇</button>
                    <div class="volume-control">
                        <input type="range" class="volume-slider" id="volumeSlider" min="0" max="100" value="0" title="Volume">
                    </div>
                    <div class="time-display" id="timeDisplay">0:00 / 0:00</div>
                    {segment_button_html}
                    {share_button_html}
                    <button class="control-btn" id="downloadBtn" title="{'View on YouTube' if is_youtube else 'Download'}">{'🎬' if is_youtube else '📥'}</button>
                    <button class="control-btn" id="fullscreenBtn" title="Fullscreen">⛶</button>
                </div>
            </div>
        </div>

        <script>
            let video = document.getElementById('customVideo');
            let youtubePlayer = null;
            let isYouTube = {str(is_youtube).lower()};
            let startTime = {start_time if start_time is not None else 'null'};
            let endTime = {end_time if end_time is not None else 'null'};
            let naturalLoop = {str(loop).lower()};
            
            const playPauseBtn = document.getElementById('playPauseBtn');
            const muteBtn = document.getElementById('muteBtn');
            const volumeSlider = document.getElementById('volumeSlider');
            const progressContainer = document.getElementById('progressContainer');
            const progressBar = document.getElementById('progressBar');
            const progressHandle = document.getElementById('progressHandle');
            const timeDisplay = document.getElementById('timeDisplay');
            const downloadBtn = document.getElementById('downloadBtn');
            const fullscreenBtn = document.getElementById('fullscreenBtn');
            const timeRange = document.getElementById('timeRange');
            const segmentBtn = document.getElementById('segmentBtn');

            let isDragging = false;
            let wasPlaying = false;
            const isAutoplay = {str(autoplay).lower()};
            let isSegmentLooping = (startTime !== null || endTime !== null); // Default to on when time constraints exist
            let segmentTimer = null;
            
            function startSegmentTimer() {{
                if (segmentTimer) clearInterval(segmentTimer);
                
                segmentTimer = setInterval(() => {{
                    let currentTime = 0;
                    
                    if (isYouTube && youtubePlayer) {{
                        try {{
                            currentTime = youtubePlayer.getCurrentTime();
                        }} catch (e) {{
                            return;
                        }}
                    }} else if (video) {{
                        currentTime = video.currentTime;
                    }} else {{
                        return;
                    }}
                    
                    if (isSegmentLooping && endTime !== null && currentTime >= endTime) {{
                        if (isYouTube && youtubePlayer) {{
                            youtubePlayer.seekTo(startTime || 0, true);
                        }} else if (video) {{
                            video.currentTime = startTime || 0;
                        }}
                    }}
                }}, 200);
            }}
            
            function stopSegmentTimer() {{
                if (segmentTimer) {{
                    clearInterval(segmentTimer);
                    segmentTimer = null;
                }}
            }}
            
            if (isYouTube) {{
                const playerId = 'youtubePlayer_' + Math.random().toString(36).substr(2, 9);
                document.getElementById('youtubePlayer').id = playerId;
                
                if (window.youtubePlayer) {{
                    try {{
                        window.youtubePlayer.destroy();
                    }} catch (e) {{
                        console.log('Error destroying previous player:', e);
                    }}
                }}
                
                function initYouTubePlayer() {{
                    youtubePlayer = new YT.Player(playerId, {{
                        height: '100%',
                        width: '100%',
                        videoId: '{extract_youtube_id(video_url) if is_youtube else ""}',
                        playerVars: {{
                            'autoplay': 0,
                            'controls': 1,
                            'rel': 0,
                            'showinfo': 0,
                            'modestbranding': 1,
                            'iv_load_policy': 3,
                            'fs': 1,
                            'loop': naturalLoop ? 1 : 0,
                            'playlist': naturalLoop ? '{extract_youtube_id(video_url) if is_youtube else ""}' : ''
                        }},
                        events: {{
                            'onReady': function(event) {{
                                window.youtubePlayer = youtubePlayer;

                                // Add these lines to mute by default
                                youtubePlayer.mute();
                                youtubePlayer.setVolume(0);
                                
                                if (startTime !== null) {{
                                    youtubePlayer.seekTo(startTime, true);
                                }}
                                
                                const duration = youtubePlayer.getDuration();
                                timeDisplay.textContent = `0:00 / ${{formatTime(duration)}}`;
                                
                                setTimeout(() => {{
                                    updateTimeRange();
                                }}, 500);
                                
                                updateProgress();
                                syncVolumeFromYouTube();
                                
                                if (isAutoplay) {{
                                    setTimeout(() => {{
                                        youtubePlayer.playVideo();
                                    }}, 1000);
                                }}
                            }},
                            'onStateChange': function(event) {{
                                syncControlsWithYouTube();
                                
                                if (event.data === 1) {{
                                    playPauseBtn.textContent = '⏸️';
                                    if (isSegmentLooping) {{
                                        startSegmentTimer();
                                    }}
                                }} else if (event.data === 2) {{
                                    playPauseBtn.textContent = '▶️';
                                    stopSegmentTimer();
                                }} else if (event.data === 0) {{
                                    playPauseBtn.textContent = '▶️';
                                    stopSegmentTimer();
                                }}
                            }},
                            'onError': function(event) {{
                                console.log('YouTube player error:', event.data);
                            }}
                        }}
                    }});
                }}
                
                if (typeof YT !== 'undefined' && YT.Player) {{
                    initYouTubePlayer();
                }} else {{
                    window.onYouTubeIframeAPIReady = initYouTubePlayer;
                }}
            }}
            
            if (!isYouTube && video) {{
                video.addEventListener('loadedmetadata', () => {{
                    if (startTime !== null) {{
                        video.currentTime = startTime;
                    }}
                    
                    const duration = formatTime(video.duration);
                    timeDisplay.textContent = `0:00 / ${{duration}}`;
                    
                    updateTimeRange();
                }});
                
                video.addEventListener('play', () => {{
                    playPauseBtn.textContent = '⏸️';
                }});
                
                video.addEventListener('pause', () => {{
                    playPauseBtn.textContent = '▶️';
                }});
                
                // Add click event listener for video play/pause
                video.addEventListener('click', () => {{
                    if (video.paused) {{
                        video.play();
                    }} else {{
                        video.pause();
                    }}
                }});
                
                if (isAutoplay) {{
                    video.addEventListener('loadeddata', () => {{
                        if (video.paused) {{
                            video.play().catch(e => console.log('Autoplay prevented:', e));
                        }}
                    }});
                }}
            }}
            
            function syncControlsWithYouTube() {{
                if (!isYouTube || !youtubePlayer) return;
                
                try {{
                    const state = youtubePlayer.getPlayerState();
                    if (state === 1) {{
                        playPauseBtn.textContent = '⏸️';
                    }} else if (state === 2 || state === 0) {{
                        playPauseBtn.textContent = '▶️';
                    }}
                    
                    syncVolumeFromYouTube();
                }} catch (e) {{
                    console.log('Error syncing controls with YouTube:', e);
                }}
            }}
            
            function syncVolumeFromYouTube() {{
                if (!isYouTube || !youtubePlayer) return;
                
                try {{
                    const volume = youtubePlayer.getVolume();
                    const isMuted = youtubePlayer.isMuted();
                    
                    volumeSlider.value = volume;
                    muteBtn.textContent = isMuted ? '🔇' : '🔊';
                }} catch (e) {{
                    console.log('Error syncing volume from YouTube:', e);
                }}
            }}
            
            function updateTimeRange() {{
                if (!isYouTube || (startTime === null && endTime === null)) {{
                    timeRange.style.display = 'none';
                    return;
                }}
                
                let duration = 0;
                
                if (isYouTube && youtubePlayer) {{
                    try {{
                        duration = youtubePlayer.getDuration();
                    }} catch (e) {{
                        return;
                    }}
                }} else {{
                    return;
                }}
                
                if (duration) {{
                    const start = startTime || 0;
                    const end = endTime || duration;
                    const startPercent = (start / duration) * 100;
                    const endPercent = (end / duration) * 100;
                    const width = endPercent - startPercent;
                    
                    timeRange.style.left = startPercent + '%';
                    timeRange.style.width = width + '%';
                    timeRange.style.display = 'block';
                }}
            }}
            
            function updateProgress() {{
                let currentTime = 0;
                let duration = 0;
                
                if (isYouTube && youtubePlayer) {{
                    try {{
                        currentTime = youtubePlayer.getCurrentTime();
                        duration = youtubePlayer.getDuration();
                    }} catch (e) {{
                        setTimeout(updateProgress, 100);
                        return;
                    }}
                }} else if (video) {{
                    currentTime = video.currentTime;
                    duration = video.duration;
                }} else {{
                    setTimeout(updateProgress, 100);
                    return;
                }}
                
                if (!isDragging && duration > 0) {{
                    const progress = (currentTime / duration) * 100;
                    progressBar.style.width = progress + '%';
                    progressHandle.style.left = progress + '%';
                    
                    const current = formatTime(currentTime);
                    const total = formatTime(duration);
                    timeDisplay.textContent = `${{current}} / ${{total}}`;
                }}
                
                setTimeout(updateProgress, 100);
            }}
            
            playPauseBtn.addEventListener('click', () => {{
                if (isYouTube && youtubePlayer) {{
                    try {{
                        if (youtubePlayer.getPlayerState() === 1) {{
                            youtubePlayer.pauseVideo();
                        }} else {{
                            youtubePlayer.playVideo();
                        }}
                    }} catch (e) {{
                        console.log('YouTube player not ready');
                    }}
                }} else if (video) {{
                    if (video.paused) {{
                        video.play();
                    }} else {{
                        video.pause();
                    }}
                }}
            }});
            
            if (segmentBtn) {{
                segmentBtn.addEventListener('click', () => {{
                    if (isSegmentLooping) {{
                        // Stop segment looping
                        isSegmentLooping = false;
                        segmentBtn.textContent = '🔄';
                        segmentBtn.classList.remove('active');
                        segmentBtn.title = 'Loop Time Segment';
                        stopSegmentTimer();
                    }} else {{
                        // Start segment looping
                        isSegmentLooping = true;
                        segmentBtn.textContent = '⏹️';
                        segmentBtn.classList.add('active');
                        segmentBtn.title = 'Stop Segment Loop';
                        
                        if (isYouTube && youtubePlayer) {{
                            youtubePlayer.seekTo(startTime || 0, true);
                            youtubePlayer.playVideo();
                        }}
                        
                        startSegmentTimer();
                    }}
                }});
            }}
            
            function getProgressFromMouse(e) {{
                const rect = progressContainer.getBoundingClientRect();
                return Math.max(0, Math.min(1, (e.clientX - rect.left) / rect.width));
            }}
            
            progressContainer.addEventListener('mousedown', (e) => {{
                isDragging = true;
                const percent = getProgressFromMouse(e);
                
                if (isYouTube && youtubePlayer) {{
                    try {{
                        const duration = youtubePlayer.getDuration();
                        const newTime = percent * duration;
                        youtubePlayer.seekTo(newTime, true);
                        
                        progressBar.style.width = (percent * 100) + '%';
                        progressHandle.style.left = (percent * 100) + '%';
                        
                        const current = formatTime(newTime);
                        const total = formatTime(duration);
                        timeDisplay.textContent = `${{current}} / ${{total}}`;
                    }} catch (e) {{
                        console.log('Error seeking YouTube video');
                    }}
                }} else if (video && video.duration) {{
                    const newTime = percent * video.duration;
                    video.currentTime = newTime;
                    
                    progressBar.style.width = (percent * 100) + '%';
                    progressHandle.style.left = (percent * 100) + '%';
                    
                    const current = formatTime(newTime);
                    const total = formatTime(video.duration);
                    timeDisplay.textContent = `${{current}} / ${{total}}`;
                }}
                
                e.preventDefault();
            }});
            
            document.addEventListener('mousemove', (e) => {{
                if (isDragging) {{
                    const percent = getProgressFromMouse(e);
                    
                    if (isYouTube && youtubePlayer) {{
                        try {{
                            const duration = youtubePlayer.getDuration();
                            const newTime = percent * duration;
                            youtubePlayer.seekTo(newTime, true);
                            
                            progressBar.style.width = (percent * 100) + '%';
                            progressHandle.style.left = (percent * 100) + '%';
                            
                            const current = formatTime(newTime);
                            const total = formatTime(duration);
                            timeDisplay.textContent = `${{current}} / ${{total}}`;
                        }} catch (e) {{
                            console.log('Error seeking YouTube video');
                        }}
                    }} else if (video && video.duration) {{
                        const newTime = percent * video.duration;
                        video.currentTime = newTime;
                        
                        progressBar.style.width = (percent * 100) + '%';
                        progressHandle.style.left = (percent * 100) + '%';
                        
                        const current = formatTime(newTime);
                        const total = formatTime(video.duration);
                        timeDisplay.textContent = `${{current}} / ${{total}}`;
                    }}
                }}
            }});
            
            document.addEventListener('mouseup', () => {{
                isDragging = false;
            }});
            
            progressContainer.addEventListener('click', (e) => {{
                if (!isDragging) {{
                    const percent = getProgressFromMouse(e);
                    
                    if (isYouTube && youtubePlayer) {{
                        try {{
                            const duration = youtubePlayer.getDuration();
                            const newTime = percent * duration;
                            youtubePlayer.seekTo(newTime, true);
                        }} catch (e) {{
                            console.log('Error seeking YouTube video');
                        }}
                    }} else if (video && video.duration) {{
                        const newTime = percent * video.duration;
                        video.currentTime = newTime;
                    }}
                }}
            }});

            muteBtn.addEventListener('click', () => {{
                if (isYouTube && youtubePlayer) {{
                    try {{
                        if (youtubePlayer.isMuted()) {{
                            youtubePlayer.unMute();
                            muteBtn.textContent = '🔊';
                        }} else {{
                            youtubePlayer.mute();
                            muteBtn.textContent = '🔇';
                        }}
                    }} catch (e) {{
                        console.log('Error toggling YouTube mute');
                    }}
                }} else if (video) {{
                    video.muted = !video.muted;
                    muteBtn.textContent = video.muted ? '🔇' : '🔊';
                }}
            }});

            volumeSlider.addEventListener('input', () => {{
                if (isYouTube && youtubePlayer) {{
                    try {{
                        youtubePlayer.setVolume(volumeSlider.value);
                        if (volumeSlider.value == 0) {{
                            muteBtn.textContent = '🔇';
                        }} else {{
                            muteBtn.textContent = '🔊';
                        }}
                    }} catch (e) {{
                        console.log('Error setting YouTube volume');
                    }}
                }} else if (video) {{
                    video.volume = volumeSlider.value / 100;
                }}
            }});
            
            if (isYouTube) {{
                setInterval(() => {{
                    if (youtubePlayer && !volumeSlider.matches(':active')) {{
                        syncVolumeFromYouTube();
                    }}
                }}, 1000);
            }}

            fullscreenBtn.addEventListener('click', () => {{
                if (isYouTube && youtubePlayer) {{
                    try {{
                        const iframe = document.querySelector('iframe');
                        if (iframe) {{
                            if (document.fullscreenElement) {{
                                document.exitFullscreen();
                            }} else {{
                                iframe.requestFullscreen();
                            }}
                        }}
                    }} catch (e) {{
                        console.log('Fullscreen error:', e);
                    }}
                }} else {{
                    if (document.fullscreenElement) {{
                        document.exitFullscreen();
                    }} else {{
                        document.querySelector('.video-wrapper').requestFullscreen();
                    }}
                }}
            }});

            {download_btn_action}

            function formatTime(time) {{
                if (isNaN(time)) return '0:00';
                const minutes = Math.floor(time / 60);
                const seconds = Math.floor(time % 60);
                return `${{minutes}}:${{seconds.toString().padStart(2, '0')}}`;
            }}

            {share_button_js}
            
            updateProgress();
            
            window.addEventListener('beforeunload', () => {{
                stopSegmentTimer();
                if (window.youtubePlayer) {{
                    try {{
                        window.youtubePlayer.destroy();
                        window.youtubePlayer = null;
                    }} catch (e) {{
                        console.log('Error destroying YouTube player:', e);
                    }}
                }}
            }});
            
            window.addEventListener('pagehide', () => {{
                stopSegmentTimer();
                if (window.youtubePlayer) {{
                    try {{
                        window.youtubePlayer.destroy();
                        window.youtubePlayer = null;
                    }} catch (e) {{
                        console.log('Error destroying YouTube player:', e);
                    }}
                }}
            }});
        </script>
    </body>
    </html>
    """
    
    estimated_width = 420
    video_height = estimated_width / aspect_ratio_decimal
    controls_height = 90
    total_height = max(500, min(700, int(video_height + controls_height)))
    
    components.html(html_code, height=total_height, scrolling=False)
    return total_height
# TODO: Original code without youtube
# import streamlit.components.v1 as components

# ###############################################################################
# # VIDEO PLAYER WITH HEIGHT RETURN
# ###############################################################################

# def custom_video_player(video_url, video_uid, aspect_ratio="16:9", autoplay=True, loop=True, show_share_button=False):
#     """Custom video player with responsive design
    
#     Args:
#         video_url: URL of the video
#         video_uid: Optional video UID for share functionality. If not provided, extracted from URL
#         aspect_ratio: Video aspect ratio
#         autoplay: Whether to autoplay
#         loop: Whether to loop
#         show_share_button: Whether to show share button
#     """
#     ratio_parts = aspect_ratio.split(":")
#     aspect_ratio_decimal = float(ratio_parts[0]) / float(ratio_parts[1])
#     padding_bottom = (1 / aspect_ratio_decimal) * 100
    
#     video_attributes = 'preload="metadata"'
#     if autoplay:
#         video_attributes += ' autoplay muted'
#     if loop:
#         video_attributes += ' loop'

#     # Prepare share button HTML/JS
#     share_button_html = ""
#     share_button_js = ""

#     if show_share_button:
        
#         share_button_html = '''<button class="control-btn" id="shareBtn" title="Copy search link">🔗</button>'''
        
#         share_button_js = f'''
#             const shareBtn = document.getElementById('shareBtn');
#             if (shareBtn) {{
#                 shareBtn.addEventListener('click', () => {{
#                     // Use parent window URL instead of iframe URL
#                     const currentUrl = window.parent.location.href.split('?')[0];
#                     const searchPortalUrl = currentUrl + '?video_uid={video_uid}';
#                     navigator.clipboard.writeText(searchPortalUrl).catch(e => console.log('Copy failed:', e));
#                 }});
#             }}
#         '''
    
#     html_code = f"""
#     <!DOCTYPE html>
#     <html>
#     <head>
#         <style>
#             * {{ box-sizing: border-box; margin: 0; padding: 0; }}
#             html, body {{ height: 100%; font-family: Arial, sans-serif; overflow: hidden; }}
            
#             .video-container {{
#                 width: 100%; height: 100%; display: flex; flex-direction: column;
#                 background: #fff; overflow: hidden;
#             }}
            
#             .video-wrapper {{
#                 position: relative; width: 100%; flex: 1; background: #000;
#                 border-radius: 8px 8px 0 0; overflow: hidden; min-height: 200px;
#             }}
            
#             .video-wrapper::before {{
#                 content: ''; display: block; padding-bottom: {padding_bottom}%;
#             }}
            
#             video {{
#                 position: absolute; top: 0; left: 0; width: 100%; height: 100%;
#                 object-fit: contain;
#             }}
            
#             video::-webkit-media-controls, video::-moz-media-controls {{
#                 display: none !important;
#             }}
            
#             .controls-container {{
#                 width: 100%; background: #f8f9fa; border: 1px solid #e9ecef;
#                 border-top: none; border-radius: 0 0 8px 8px; padding: 8px 12px;
#                 flex-shrink: 0; overflow: hidden; min-height: 65px; max-height: 80px;
#             }}
            
#             .progress-container {{
#                 width: 100%; height: 6px; background: #ddd; border-radius: 3px;
#                 margin-bottom: 8px; cursor: pointer; position: relative;
#                 user-select: none; overflow: hidden;
#             }}
            
#             .progress-bar {{
#                 height: 100%; background: #9553FE;
#                 border-radius: 3px; width: 0%; pointer-events: none; transition: none;
#             }}
            
#             .progress-handle {{
#                 position: absolute; top: -5px; width: 16px; height: 16px;
#                 background: #9553FE; border: 2px solid white; border-radius: 50%;
#                 cursor: grab; transform: translateX(-50%); opacity: 0;
#                 transition: opacity 0.2s ease, transform 0.1s ease;
#                 box-shadow: 0 2px 4px rgba(0,0,0,0.2);
#             }}
            
#             .progress-handle:active {{ cursor: grabbing; transform: translateX(-50%) scale(1.1); }}
#             .progress-container:hover .progress-handle {{ opacity: 1; }}
            
#             .controls {{
#                 display: flex; align-items: center; gap: 6px; width: 100%;
#                 overflow: hidden; min-height: 32px;
#             }}
            
#             .control-btn {{
#                 background: none; border: none; font-size: 14px; cursor: pointer;
#                 padding: 4px 6px; border-radius: 4px; transition: background 0.2s ease;
#                 display: flex; align-items: center; justify-content: center;
#                 min-width: 28px; height: 28px; flex-shrink: 0;
#             }}
            
#             .control-btn:hover {{ background: #e9ecef; }}
            
#             .time-display {{
#                 font-size: 11px; color: #666; margin-left: auto; white-space: nowrap;
#                 font-family: 'Courier New', monospace; flex-shrink: 0;
#                 overflow: hidden; text-overflow: ellipsis; max-width: 120px;
#             }}
            
#             .volume-control {{ display: flex; align-items: center; gap: 4px; flex-shrink: 0; }}
            
#             .volume-slider {{
#                 width: 50px; height: 3px; background: #ddd; outline: none;
#                 border-radius: 2px; -webkit-appearance: none; flex-shrink: 0;
#             }}
            
#             .volume-slider::-webkit-slider-thumb {{
#                 -webkit-appearance: none; width: 12px; height: 12px;
#                 background: #9553FE; border-radius: 50%; cursor: pointer;
#             }}
            
#             .volume-slider::-moz-range-thumb {{
#                 width: 12px; height: 12px; background: #9553FE;
#                 border-radius: 50%; cursor: pointer; border: none;
#             }}
            
#             @media (max-width: 600px) {{
#                 .controls {{ gap: 4px; }}
#                 .control-btn {{ font-size: 12px; min-width: 24px; height: 24px; padding: 2px 4px; }}
#                 .time-display {{ font-size: 10px; max-width: 80px; }}
#                 .volume-slider {{ width: 40px; }}
#                 .controls-container {{ padding: 6px 8px; min-height: 60px; }}
#                 .progress-container {{ height: 5px; margin-bottom: 6px; }}
#             }}
#         </style>
#     </head>
#     <body>
#         <div class="video-container">
#             <div class="video-wrapper">
#                 <video id="customVideo" {video_attributes}>
#                     <source src="{video_url}" type="video/mp4">
#                     Your browser does not support the video tag.
#                 </video>
#             </div>
            
#             <div class="controls-container">
#                 <div class="progress-container" id="progressContainer">
#                     <div class="progress-bar" id="progressBar"></div>
#                     <div class="progress-handle" id="progressHandle"></div>
#                 </div>
                
#                 <div class="controls">
#                     <button class="control-btn" id="playPauseBtn" title="Play/Pause">{"⏸️" if autoplay else "▶️"}</button>
#                     <button class="control-btn" id="muteBtn" title="Mute/Unmute">🔊</button>
#                     <div class="volume-control">
#                         <input type="range" class="volume-slider" id="volumeSlider" min="0" max="100" value="100" title="Volume">
#                     </div>
#                     <div class="time-display" id="timeDisplay">0:00 / 0:00</div>
#                     {share_button_html}
#                     <button class="control-btn" id="downloadBtn" title="Download">📥</button>
#                     <button class="control-btn" id="fullscreenBtn" title="Fullscreen">⛶</button>
#                 </div>
#             </div>
#         </div>

#         <script>
#             const video = document.getElementById('customVideo');
#             const playPauseBtn = document.getElementById('playPauseBtn');
#             const muteBtn = document.getElementById('muteBtn');
#             const volumeSlider = document.getElementById('volumeSlider');
#             const progressContainer = document.getElementById('progressContainer');
#             const progressBar = document.getElementById('progressBar');
#             const progressHandle = document.getElementById('progressHandle');
#             const timeDisplay = document.getElementById('timeDisplay');
#             const downloadBtn = document.getElementById('downloadBtn');
#             const fullscreenBtn = document.getElementById('fullscreenBtn');

#             let isDragging = false;
#             let wasPlaying = false;
#             const isAutoplay = {str(autoplay).lower()};
            
#             if (isAutoplay) {{
#                 video.addEventListener('loadeddata', () => {{
#                     if (video.paused) {{
#                         video.play().catch(e => console.log('Autoplay prevented:', e));
#                     }}
#                     playPauseBtn.textContent = video.paused ? '▶️' : '⏸️';
#                 }});
#             }}

#             playPauseBtn.addEventListener('click', () => {{
#                 if (video.paused) {{
#                     video.play();
#                     playPauseBtn.textContent = '⏸️';
#                 }} else {{
#                     video.pause();
#                     playPauseBtn.textContent = '▶️';
#                 }}
#             }});

#             muteBtn.addEventListener('click', () => {{
#                 video.muted = !video.muted;
#                 muteBtn.textContent = video.muted ? '🔇' : '🔊';
#             }});

#             volumeSlider.addEventListener('input', () => {{
#                 video.volume = volumeSlider.value / 100;
#             }});

#             function updateProgress() {{
#                 if (!isDragging && video.duration) {{
#                     const progress = (video.currentTime / video.duration) * 100;
#                     progressBar.style.width = progress + '%';
#                     progressHandle.style.left = progress + '%';
                    
#                     const currentTime = formatTime(video.currentTime);
#                     const duration = formatTime(video.duration);
#                     timeDisplay.textContent = `${{currentTime}} / ${{duration}}`;
#                 }}
#                 requestAnimationFrame(updateProgress);
#             }}
            
#             updateProgress();

#             function getProgressFromMouse(e) {{
#                 const rect = progressContainer.getBoundingClientRect();
#                 return Math.max(0, Math.min(1, (e.clientX - rect.left) / rect.width));
#             }}

#             progressContainer.addEventListener('mousedown', (e) => {{
#                 isDragging = true;
#                 wasPlaying = !video.paused;
#                 if (wasPlaying) video.pause();
                
#                 const percent = getProgressFromMouse(e);
#                 const newTime = percent * video.duration;
                
#                 progressBar.style.width = (percent * 100) + '%';
#                 progressHandle.style.left = (percent * 100) + '%';
#                 video.currentTime = newTime;
                
#                 const currentTime = formatTime(newTime);
#                 const duration = formatTime(video.duration);
#                 timeDisplay.textContent = `${{currentTime}} / ${{duration}}`;
                
#                 e.preventDefault();
#             }});

#             document.addEventListener('mousemove', (e) => {{
#                 if (isDragging) {{
#                     const percent = getProgressFromMouse(e);
#                     const newTime = percent * video.duration;
                    
#                     progressBar.style.width = (percent * 100) + '%';
#                     progressHandle.style.left = (percent * 100) + '%';
#                     video.currentTime = newTime;
                    
#                     const currentTime = formatTime(newTime);
#                     const duration = formatTime(video.duration);
#                     timeDisplay.textContent = `${{currentTime}} / ${{duration}}`;
#                 }}
#             }});

#             document.addEventListener('mouseup', () => {{
#                 if (isDragging) {{
#                     isDragging = false;
#                     if (wasPlaying) video.play();
#                 }}
#             }});

#             progressContainer.addEventListener('click', (e) => {{
#                 if (!isDragging) {{
#                     const percent = getProgressFromMouse(e);
#                     video.currentTime = percent * video.duration;
#                 }}
#             }});

#             fullscreenBtn.addEventListener('click', () => {{
#                 if (document.fullscreenElement) {{
#                     document.exitFullscreen();
#                 }} else {{
#                     document.querySelector('.video-wrapper').requestFullscreen();
#                 }}
#             }});

#             downloadBtn.addEventListener('click', async () => {{
#                 try {{
#                     const response = await fetch('{video_url}');
#                     const blob = await response.blob();
#                     const url = window.URL.createObjectURL(blob);
#                     const a = document.createElement('a');
#                     a.href = url;
#                     a.download = '{video_url}'.split('/').pop();
#                     a.style.display = 'none';
#                     document.body.appendChild(a);
#                     a.click();
#                     document.body.removeChild(a);
#                     window.URL.revokeObjectURL(url);
#                 }} catch (error) {{
#                     console.error('Download failed:', error);
#                     // Fallback to direct link
#                     const a = document.createElement('a');
#                     a.href = '{video_url}';
#                     a.download = '{video_url}'.split('/').pop();
#                     a.target = '_blank';
#                     a.click();
#                 }}
#             }});

#             function formatTime(time) {{
#                 if (isNaN(time)) return '0:00';
#                 const minutes = Math.floor(time / 60);
#                 const seconds = Math.floor(time % 60);
#                 return `${{minutes}}:${{seconds.toString().padStart(2, '0')}}`;
#             }}

#             video.addEventListener('ended', () => {{ playPauseBtn.textContent = '▶️'; }});
#             video.addEventListener('play', () => {{ playPauseBtn.textContent = '⏸️'; }});
#             video.addEventListener('pause', () => {{ playPauseBtn.textContent = '▶️'; }});

#             video.addEventListener('loadedmetadata', () => {{
#                 const duration = formatTime(video.duration);
#                 timeDisplay.textContent = `0:00 / ${{duration}}`;
#             }});

#             // ADD THIS at the end of the JavaScript section:
#             {share_button_js}
#         </script>
#     </body>
#     </html>
#     """
    
#     estimated_width = 420
#     video_height = estimated_width / aspect_ratio_decimal
#     controls_height = 90
#     total_height = max(500, min(700, int(video_height + controls_height)))
    
#     components.html(html_code, height=total_height, scrolling=False)
#     return total_height
