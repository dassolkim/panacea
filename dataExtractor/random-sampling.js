const fh = require('../fileHandler/file-handler')

module.exports = { randomSampling, randomSamplingWithLimit, randomSamplingWithFairness, randomNum }

function randomSampling(end, dataDir, urlInfo, limit) {

    const randomList = []
    let cnt = 0

    while (true) {
        const random = Math.floor(Math.random() * end + 1)
        if (randomList.indexOf(random) === -1) {
            randomList.push(random)
        }
        urlInfo.page = random
        const rUrls = fh.readUrls(dataDir, urlInfo)
        if (rUrls == false) {
            console.log(`Zero ${urlInfo.format} files in catalog page ${urlInfo.page}`)
            randomList.pop(random)
        } else {
            const urlObj = JSON.parse(rUrls)
            const count = urlObj.info.count
            if (count == 0) {
                randomList.pop(random)
            } else if (count > limit) {
                randomList.pop(random)
            } else {
                cnt += count
                if (cnt < limit) {
                    // cnt += count
                    continue
                } else {
                    console.log(`Number of files in random list: ${cnt}`)
                    // console.log(randomList)
                    break
                }
            }
        }
    }
    // console.log(``)
    return randomList
}

function randomSamplingWithLimit(end, dataDir, urlInfo, limit) {

    const randomList = []
    let cnt = 0

    while (true) {
        const random = Math.floor(Math.random() * end + 1)
        if (randomList.indexOf(random) === -1) {
            randomList.push(random)
        }
        urlInfo.page = random
        const rUrls = fh.readUrls(dataDir, urlInfo)
        if (rUrls == false) {
            console.log(`Zero ${urlInfo.format} files in catalog page ${urlInfo.page}`)
            randomList.pop(random)
        } else {
            const urlObj = JSON.parse(rUrls)
            const count = urlObj.info.count
            if (count == 0) {
                randomList.pop(random)
            } else if (count > limit) {
                randomList.pop(random)
            } else {
                cnt += count
                if (limit <= cnt && cnt <= (limit * 1.1)) {
                    console.log(`Number of files in random list: ${cnt}`)
                    break
                } else {
                    randomList.pop(random)
                    cnt -= count
                    // console.log(randomList)
                    // break
                }
            }
        }
    }
    console.log(`Number of dataset in randomList: ${cnt}`)
    return randomList
}

function randomSamplingWithFairness(end, dataDir, urlInfo, limit) {

    const firstList = []
    
    let cnt = 0
    let fcnt = 0
    const mid = end / 2
    while (true) {
        if (fcnt < 5) {
            const random = randomNum(1, mid)
            if (firstList.indexOf(random) === -1) {
                firstList.push(random)
                fcnt++
            }
            urlInfo.page = random
            const rUrls = fh.readUrls(dataDir, urlInfo)
            if (rUrls == false) {
                // console.log(`Zero ${urlInfo.format} files in catalog page ${urlInfo.page}`)
                firstList.pop(random)
                fcnt--
            } else {
                const urlObj = JSON.parse(rUrls)
                const count = urlObj.info.count
                if (count == 0) {
                    firstList.pop(random)
                    fcnt--

                } else if (count > limit || count > mid || count < (limit * 0.1)) {
                    firstList.pop(random)
                    fcnt--

                } else {
                    cnt += count
                    if (limit <= cnt && cnt <= (limit * 1.1)) {
                        console.log(`Number of files in random list: ${cnt}`)
                        break
                    }
                }
            }
        } else {
            break
        }
    }
    const lastList = []
    let ccnt = 0
    let lcnt = 0
    while (true) {
        if (lcnt < 5) {
            const random = randomNum(mid, end)
            if (lastList.indexOf(random) === -1) {
                lastList.push(random)
                lcnt++
            }
            urlInfo.page = random
            const rUrls = fh.readUrls(dataDir, urlInfo)
            if (rUrls == false) {
                // console.log(`Zero ${urlInfo.format} files in catalog page ${urlInfo.page}`)
                lastList.pop(random)
                lcnt--
            } else {
                const urlObj = JSON.parse(rUrls)
                const count = urlObj.info.count
                if (count == 0) {
                    lastList.pop(random)
                    lcnt--

                } else if (count > limit || count > mid || count < (limit * 0.1)) {
                    lastList.pop(random)
                    lcnt--

                } else {
                    ccnt += count
                    if (limit <= ccnt && ccnt <= (limit * 1.1)) {
                        console.log(`Number of files in random list: ${ccnt}`)
                        break
                    }
                }
            }
        } else {
            break
        }
    }
    const finalList = [
        ...firstList,
        ...lastList
    ]
    console.log(`Number of dataset in randomList: ${cnt + ccnt}`)
    return finalList
}

function randomNum(min, max) {
    const randNum = Math.floor(Math.random() * (max - min + 1)) + min
    return randNum
}

