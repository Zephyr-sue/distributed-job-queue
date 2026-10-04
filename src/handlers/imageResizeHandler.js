const { sleep } = require('../utils/helpers');

async function handleImageResize(payload, job) {
  const { imageUrl, dimensions = { width: 800, height: 600 } } = payload;
  if (!imageUrl) throw new Error('imageUrl is required for IMAGE_RESIZE job');

  // Simulate CPU image compression and transform
  await sleep(200 + Math.random() * 250);

  return {
    originalUrl: imageUrl,
    resizedUrl: `${imageUrl}_resized_${dimensions.width}x${dimensions.height}.jpg`,
    dimensions,
    fileSizeBytes: Math.floor(Math.random() * 200000) + 50000,
  };
}

module.exports = { handleImageResize };
