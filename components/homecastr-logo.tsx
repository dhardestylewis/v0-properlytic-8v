import Image from "next/image"

interface HomecastrLogoProps {
    className?: string
    size?: number
    variant?: "icon" | "horizontal"
}

export function HomecastrLogo({ className = "", size = 32, variant = "icon" }: HomecastrLogoProps) {
    if (variant === "horizontal") {
        // Horizontal logo aspect ratio (563x127)
        const height = size
        const width = Math.round(height * (563 / 127))
        return (
            <Image
                src="/homecastr-logo-horizontal.png"
                alt="Homecastr"
                width={width}
                height={height}
                className={className}
                style={{ objectFit: "contain" }}
            />
        )
    }
    return (
        <Image
            src="/homecastr-icon.png"
            alt="Homecastr"
            width={size}
            height={size}
            className={className}
            style={{ objectFit: "contain" }}
        />
    )
}
